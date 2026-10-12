/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.exp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalTime;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.raven.CatalogOuterClass;
import org.apache.iceberg.raven.CatalogOuterClass.FileObject;
import org.apache.iceberg.raven.CatalogOuterClass.TableObject;
import org.apache.iceberg.raven.RavenCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * MultiTableTxn4 benchmarks multi-table transactions for insert-only workloads
 * supporting both Raven and DuckLake (with PostgreSQL catalog metadata and S3 storage).
 */
public class MultiTableTxn4 {

    private MultiTableTxn4() {}

    /**
     * Thread-safe Zipfian generator for picking tables at random according to skew.
     */
    public static class ZipfGenerator {
        private final ThreadLocal<ZipfDistribution> threadLocalZipf;

        public ZipfGenerator(int numTables, double skew) {
            if (numTables <= 0 || skew <= 0.0) {
                throw new IllegalArgumentException("numTables and skew must be > 0");
            }
            this.threadLocalZipf = ThreadLocal.withInitial(
                    () -> new ZipfDistribution(numTables, skew)
            );
        }

        public int getNext() {
            return threadLocalZipf.get().sample();
        }
    }

    /**
     * Stores the start and end timestamps of an operation.
     */
    public static class TimePair {
        public final Instant start;
        public final Instant end;

        public TimePair(Instant start, Instant end) {
            this.start = start;
            this.end = end;
        }

        public long getDurationMillis() {
            return end.toEpochMilli() - start.toEpochMilli();
        }
    }

    /**
     * Exports per-transaction logs and overall summary to JSON.
     */
    public static class MetricsExporter {
        public static void exportMetricsToLog(String outputFile) {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

            try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile), StandardCharsets.UTF_8)) {
                for (int i = 0; i < TXN_TIMES.size(); i++) {
                    for (int j = 0; j < TXN_TIMES.get(i).size(); j++) {
                        ObjectNode row = mapper.createObjectNode();
                        row.put("threadNum", i);
                        row.put("catalog_type", isDuckLake ? "ducklake" : "raven");
                        row.put("success", OP_SUCCESS.get(i).get(j));
                        row.put("type", "insert");
                        addTimeBlock(mapper, row, "txn_time", TXN_TIMES.get(i).get(j));
                        row.put("duration_ms", TXN_TIMES.get(i).get(j).getDurationMillis());

                        String jsonLine = mapper.writeValueAsString(row);
                        writer.write(jsonLine);
                        writer.newLine();
                    }
                }
            } catch (IOException e) {
                LOG.info("IO Error during result output", e);
            }
        }

        public static void appendSummaryToJson(String outputFile) {
            ObjectMapper mapper = new ObjectMapper();

            int numTxn = 0;
            for (List<TimePair> txnTimes : TXN_TIMES) {
                numTxn += txnTimes.size();
            }

            double totalLatencyMillis = 0;
            for (int i = 0; i < TXN_TIMES.size(); i++) {
                for (int j = 0; j < TXN_TIMES.get(i).size(); j++) {
                    totalLatencyMillis += TXN_TIMES.get(i).get(j).getDurationMillis();
                }
            }

            int numSuccess = 0;
            int numFailure = 0;
            for (List<Boolean> opSuccessList : OP_SUCCESS) {
                for (Boolean success : opSuccessList) {
                    if (success) {
                        numSuccess += 1;
                    } else {
                        numFailure += 1;
                    }
                }
            }

            double avgLatency = (numTxn == 0) ? 0 : (totalLatencyMillis / numTxn);

            ObjectNode summaryNode = mapper.createObjectNode();
            summaryNode.put("exp_name", "MultiTableTxn4");
            summaryNode.put("catalog_type", isDuckLake ? "ducklake" : "raven");
            summaryNode.put("duration", durationStr);
            summaryNode.put("num_threads", numThreads);
            summaryNode.put("num_tables", numTables);
            summaryNode.put("num_txn", numTxn);
            summaryNode.put("num_success", numSuccess);
            summaryNode.put("num_failure", numFailure);
            summaryNode.put("avg_latency", avgLatency);

            try (BufferedWriter writer = Files.newBufferedWriter(
                    Paths.get(outputFile),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND)) {
                String jsonLine = mapper.writeValueAsString(summaryNode);
                writer.write(jsonLine);
                writer.newLine();
            } catch (IOException e) {
                LOG.info("IO Error during result output", e);
            }
        }

        private static void addTimeBlock(ObjectMapper mapper, ObjectNode parent, String fieldName, TimePair timePair) {
            if (timePair == null) {
                return;
            }
            ObjectNode timeNode = mapper.createObjectNode();
            timeNode.put("start", timePair.start.toString());
            timeNode.put("end", timePair.end.toString());
            parent.set(fieldName, timeNode);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(MultiTableTxn4.class);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final int DEFAULT_NUM_TABLES = 1000;
    private static final int MAX_NUM_TABLES_PER_OP = 5;

    // Experiment config variables
    private static boolean isDuckLake;
    private static int numTables = DEFAULT_NUM_TABLES;
    private static float skew;
    private static String durationStr;
    private static String workspaceName;
    private static String dbName;
    private static String tableName;
    private static String columnName;
    private static int numThreads;
    private static int numRowsPerFile;
    private static String warehouseLocation;
    private static String s3Secret;
    private static String s3KeyId;
    private static String s3Region;
    private static String s3Endpoint;
    private static S3Client s3;
    private static String ravenAddress;
    private static ZipfGenerator zipfGen;

    // Single-column schema to minimize file-level statistics overhead
    private static Schema schema;

    // DuckLake PostgreSQL connection parameters
    private static String pgHost;
    private static String pgPort;
    private static String pgUser;
    private static String pgPassword;
    private static String pgDbName;
    private static String ducklakeDataPath;
    private static String ducklakeExtensionPath;

    // Catalogs and connections
    private static final List<RavenCatalog> RAVEN_CATALOGS = Lists.newArrayList();
    private static final List<DuckDBConnection> DUCK_DB_CONNECTIONS = Lists.newArrayList();

    // End-to-end measurements collected per thread
    private static final List<List<TimePair>> TXN_TIMES = Lists.newArrayList();
    private static final List<List<Boolean>> OP_SUCCESS = Lists.newArrayList();

    public static Map<String, String> parseJsonToMap(String jsonFilePath) throws IOException {
        File file = new File(jsonFilePath);
        return JSON_MAPPER.readValue(file, new TypeReference<Map<String, String>>() {});
    }

    /**
     * Initializes Raven catalog and AWS S3 client (used only when is_duck_lake is false).
     */
    public static void initRavenCatalog() {
        Map<String, String> conf = Maps.newHashMap();
        conf.put(CatalogProperties.URI, "thrift://localhost:9083");
        conf.put(
                CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                String.valueOf(TimeUnit.SECONDS.toMillis(10)));
        conf.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
        conf.put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName());
        conf.put(S3FileIOProperties.ACCESS_KEY_ID, s3KeyId);
        conf.put(S3FileIOProperties.SECRET_ACCESS_KEY, s3Secret);
        conf.put(AwsClientProperties.CLIENT_REGION, s3Region);
        conf.put("raven_address", ravenAddress);
        conf.put("workspace_name", workspaceName);

        AwsClientFactories.defaultFactory().initialize(conf);
        s3 = AwsClientFactories.defaultFactory().s3();
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> expConfigs = parseJsonToMap(args[0]);

        // Catalog type flag: DuckLake vs Raven
        String isDuckLakeStr = expConfigs.get("is_duck_lake");
        isDuckLake = isDuckLakeStr != null && (isDuckLakeStr.equalsIgnoreCase("true") || isDuckLakeStr.equals("1"));

        skew = Float.parseFloat(expConfigs.get("skew"));
        durationStr = expConfigs.get("duration");
        workspaceName = expConfigs.getOrDefault("workspace_name", "public");
        dbName = expConfigs.getOrDefault("db_name", "retail");
        tableName = expConfigs.getOrDefault("table_name", "store_sales");
        columnName = expConfigs.getOrDefault("column_name", "col1");

        // Single integer column schema
        schema = new Schema(Types.NestedField.required(1, columnName, Types.IntegerType.get()));

        if (expConfigs.containsKey("num_tables")) {
            numTables = Integer.parseInt(expConfigs.get("num_tables"));
        } else {
            numTables = DEFAULT_NUM_TABLES;
        }

        numThreads = Integer.parseInt(expConfigs.get("num_threads"));
        numRowsPerFile = Integer.parseInt(expConfigs.get("num_rows_per_file"));
        warehouseLocation = expConfigs.get("warehouse_location");
        s3Secret = expConfigs.get("s3_secret");
        s3KeyId = expConfigs.get("s3_key_id");
        s3Region = expConfigs.get("s3_region");
        s3Endpoint = expConfigs.get("s3_endpoint");

        if (isDuckLake) {
            pgHost = expConfigs.getOrDefault("pg_host", expConfigs.getOrDefault("db_host", "127.0.0.1"));
            pgPort = expConfigs.getOrDefault("pg_port", expConfigs.getOrDefault("db_port", "5432"));
            pgUser = expConfigs.getOrDefault("pg_user", expConfigs.getOrDefault("db_user", "postgres"));
            pgPassword = expConfigs.getOrDefault("pg_password", expConfigs.getOrDefault("db_password", ""));
            pgDbName = expConfigs.getOrDefault("pg_db_name", expConfigs.getOrDefault("pg_dbname", expConfigs.getOrDefault("db_name", "ducklakedb")));
            ducklakeDataPath = expConfigs.getOrDefault("ducklake_data_path", warehouseLocation);
            ducklakeExtensionPath = expConfigs.get("ducklake_extension_path");
        } else {
            ravenAddress = expConfigs.get("raven_address");
            initRavenCatalog();
            int numRaven = Math.min(numThreads, 6);
            for (int i = 0; i < numRaven; i++) {
                RAVEN_CATALOGS.add(new RavenCatalog(ravenAddress));
            }
        }

        zipfGen = new ZipfGenerator(numTables, skew);

        // Initialize thread 0 connection first
        DUCK_DB_CONNECTIONS.add(createDuckDBConnection());

        // In case of DuckLake, create all 1000 tables following the single column schema before starting
        if (isDuckLake) {
            createDuckLakeTables(DUCK_DB_CONNECTIONS.get(0));
        }

        // Initialize remaining per-thread DuckDB instances
        for (int i = 1; i < numThreads; i++) {
            DUCK_DB_CONNECTIONS.add(createDuckDBConnection());
        }

        // Initialize per-thread measurement lists
        for (int i = 0; i < numThreads; i++) {
            TXN_TIMES.add(Lists.newArrayList());
            OP_SUCCESS.add(Lists.newArrayList());
        }

        runExp(expConfigs);
    }

    /**
     * Creates and sets up a per-thread DuckDB connection.
     * Loads httpfs, parquet, and S3 credentials. If is_duck_lake is true, also loads
     * PostgreSQL scanner, DuckLake, disables data inlining (ducklake_default_data_inlining_row_limit = 0),
     * and attaches the DuckLake catalog.
     */
    private static DuckDBConnection createDuckDBConnection() throws SQLException {
        DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("INSTALL httpfs; LOAD httpfs;");
            stmt.execute("INSTALL parquet; LOAD parquet;");

            // Configure S3 secret for S3 storage access
            String secretSql;
            if (s3Endpoint != null && !s3Endpoint.isEmpty()) {
                secretSql = String.format(
                        Locale.getDefault(),
                        "CREATE SECRET IF NOT EXISTS s3_secret (TYPE S3, KEY_ID '%s', SECRET '%s', REGION '%s', ENDPOINT '%s', USE_SSL false);",
                        s3KeyId,
                        s3Secret,
                        s3Region,
                        s3Endpoint);
            } else {
                secretSql = String.format(
                        Locale.getDefault(),
                        "CREATE SECRET IF NOT EXISTS s3_secret (TYPE S3, KEY_ID '%s', SECRET '%s', REGION '%s');",
                        s3KeyId,
                        s3Secret,
                        s3Region);
            }
            stmt.execute(secretSql);

            if (isDuckLake) {
                // Install and load PostgreSQL scanner extension
                try {
                    stmt.execute("INSTALL postgres_scanner; LOAD postgres_scanner;");
                } catch (SQLException e) {
                    LOG.warn("Failed to load postgres_scanner, trying postgres", e);
                    stmt.execute("INSTALL postgres; LOAD postgres;");
                }

                // Install and load DuckLake extension
                try {
                    stmt.execute("INSTALL ducklake; LOAD ducklake;");
                } catch (SQLException e) {
                    LOG.warn("Failed to INSTALL/LOAD ducklake, trying core_nightly", e);
                    try {
                        stmt.execute("FORCE INSTALL ducklake FROM core_nightly; LOAD ducklake;");
                    } catch (SQLException e2) {
                        if (ducklakeExtensionPath != null && !ducklakeExtensionPath.isEmpty()) {
                            stmt.execute(String.format("LOAD '%s';", ducklakeExtensionPath));
                        } else {
                            throw e2;
                        }
                    }
                }

                // Disable data inlining globally in DuckLake so each insert creates a new file in S3
                stmt.execute("SET ducklake_default_data_inlining_row_limit = 0;");

                // Attach DuckLake catalog backed by PostgreSQL metadata and S3 data path
                StringBuilder connParams = new StringBuilder();
                connParams.append(String.format("dbname=%s host=%s port=%s user=%s", pgDbName, pgHost, pgPort, pgUser));
                if (pgPassword != null && !pgPassword.isEmpty()) {
                    connParams.append(" password=").append(pgPassword);
                }

                String attachSql = String.format(
                        Locale.getDefault(),
                        "ATTACH 'ducklake:postgres:%s' AS ducklake (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT 0);",
                        connParams.toString(),
                        ducklakeDataPath);
                stmt.execute(attachSql);

                try {
                    stmt.execute("CALL ducklake.set_option('data_inlining_row_limit', 0);");
                } catch (SQLException e) {
                    LOG.debug("Setting option via CALL ducklake.set_option was not needed or not supported", e);
                }

                try {
                    stmt.execute("USE ducklake;");
                } catch (SQLException e) {
                    LOG.debug("Setting default schema to ducklake failed", e);
                }
            }
        }
        return conn;
    }

    /**
     * Creates all 1000 tables following the single integer column schema in DuckLake
     * before starting the multi-threaded experiment.
     */
    private static void createDuckLakeTables(DuckDBConnection conn) throws SQLException {
        LOG.info("Creating {} DuckLake tables with single integer column schema ({}) in catalog 'ducklake'...",
                numTables, columnName);
        try (Statement stmt = conn.createStatement()) {
            for (int i = 1; i <= numTables; i++) {
                String targetTable = tableName + i;
                String createSql = String.format(
                        Locale.getDefault(),
                        "CREATE TABLE IF NOT EXISTS ducklake.%s (%s INTEGER);",
                        targetTable,
                        columnName);
                stmt.execute(createSql);
                if (i % 100 == 0 || i == numTables) {
                    LOG.info("Created {}/{} DuckLake tables", i, numTables);
                }
            }
        }
        LOG.info("Successfully created all {} DuckLake tables.", numTables);
    }

    private static String schemaToTargetList(Schema targetSchema) {
        List<Types.NestedField> columns = targetSchema.columns();
        StringBuilder sb = new StringBuilder();
        for (Types.NestedField column : columns) {
            switch (column.type().typeId()) {
                case INTEGER:
                    sb.append(String.format(Locale.getDefault(), "x::INTEGER AS %s,", column.name()));
                    break;
                case STRING:
                    sb.append(String.format(Locale.getDefault(), "x::VARCHAR AS %s,", column.name()));
                    break;
                default:
                    break;
            }
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    private static void runExp(Map<String, String> expConfigs) {
        LocalTime duration = LocalTime.parse(durationStr);
        runExpImpl(duration);

        String expResultDir = expConfigs.get("exp_result_dir");
        String catalogTypeStr = isDuckLake ? "ducklake" : "raven";

        String logFileName = String.format(
                Locale.getDefault(),
                "%s/multitabletxn4-%s-%d-%d-log.json",
                expResultDir,
                catalogTypeStr,
                numThreads,
                Math.round(skew * 10));
        String summaryFileName = String.format("%s/summary.json", expResultDir);

        MetricsExporter.exportMetricsToLog(logFileName);
        MetricsExporter.appendSummaryToJson(summaryFileName);
    }

    private static void runExpImpl(LocalTime duration) {
        runTaskForDuration((running, threadNum) -> {
            DuckDBConnection duckDbConn = DUCK_DB_CONNECTIONS.get(threadNum);
            RavenCatalog ravenCatalog = isDuckLake ? null : RAVEN_CATALOGS.get(threadNum % RAVEN_CATALOGS.size());

            // Worker loop: perform insert transactions until duration expires
            while (running.get()) {
                if (isDuckLake) {
                    insertOpDuckLake(duckDbConn, threadNum);
                } else {
                    insertOpRaven(duckDbConn, ravenCatalog, threadNum);
                }
            }
        }, duration.toSecondOfDay(), TimeUnit.SECONDS);
    }

    /**
     * Executes a multi-table insert transaction in DuckLake.
     * Wrapped in START TRANSACTION and COMMIT directly within DuckDB.
     */
    private static void insertOpDuckLake(DuckDBConnection duckDbConn, Integer threadNum) {
        int numTablesToTouch = ThreadLocalRandom.current().nextInt(1, MAX_NUM_TABLES_PER_OP + 1);

        // Generate tables according to Zipfian distribution
        List<Integer> tableNumList = Stream.generate(zipfGen::getNext)
                .distinct()
                .limit(numTablesToTouch)
                .collect(Collectors.toList());

        Instant beforeTxn = Instant.now();
        boolean success = false;

        try (Statement stmt = duckDbConn.createStatement()) {
            String targetList = schemaToTargetList(schema);

            // Wrap multi-table transaction in START TRANSACTION and COMMIT
            stmt.execute("START TRANSACTION;");

            for (Integer tableNum : tableNumList) {
                String targetTable = tableName + tableNum;
                // Insert directly into the DuckLake table backed by S3 storage
                stmt.execute(
                        String.format(
                                Locale.getDefault(),
                                "INSERT INTO ducklake.%s SELECT %s FROM generate_series(1, %d) AS t(x);",
                                targetTable,
                                targetList,
                                numRowsPerFile));
            }

            stmt.execute("COMMIT;");
            success = true;
        } catch (SQLException e) {
            LOG.warn("DuckLake transaction failed, executing rollback", e);
            try (Statement rollbackStmt = duckDbConn.createStatement()) {
                rollbackStmt.execute("ROLLBACK;");
            } catch (SQLException rollbackEx) {
                LOG.warn("Rollback execution failed", rollbackEx);
            }
            success = false;
        }

        Instant afterTxn = Instant.now();

        // Record end-to-end metrics
        TXN_TIMES.get(threadNum).add(new TimePair(beforeTxn, afterTxn));
        OP_SUCCESS.get(threadNum).add(success);
    }

    /**
     * Executes a multi-table insert transaction in Raven.
     * Table metadata is committed via Raven catalog, and Parquet data files are written to S3.
     */
    private static void insertOpRaven(DuckDBConnection duckDbConn, RavenCatalog ravenCatalog, Integer threadNum) {
        try (Statement stmt = duckDbConn.createStatement()) {
            int numTablesToTouch = ThreadLocalRandom.current().nextInt(1, MAX_NUM_TABLES_PER_OP + 1);

            // Generate tables according to Zipfian distribution
            List<Integer> tableNumList = Stream.generate(zipfGen::getNext)
                    .distinct()
                    .limit(numTablesToTouch)
                    .collect(Collectors.toList());

            Instant beforeTxn = Instant.now();

            List<String> tableNames = Lists.newArrayList();
            for (Integer tableNum : tableNumList) {
                tableNames.add(tableName + tableNum);
            }

            // Start txn on Raven
            CatalogOuterClass.StartTransactionResponse ravenTxn = ravenCatalog.startTransaction(
                    workspaceName, dbName, tableNames);

            long txnId = ravenTxn.getTxnId();
            List<TableObject> unsortedTableObjects = ravenTxn.getTablesList();

            List<TableObject> tableObjects = Lists.newArrayList();
            for (String name : tableNames) {
                Optional<TableObject> tableObject = unsortedTableObjects.stream()
                        .filter(t -> t.getTableName().equals(name))
                        .findFirst();
                tableObjects.add(tableObject.get());
            }

            List<String> filePaths = Lists.newArrayList();
            String targetList = schemaToTargetList(schema);

            for (int j = 0; j < tableNumList.size(); j++) {
                // Generate data and write Parquet file to S3
                stmt.execute(
                        String.format(
                                Locale.getDefault(),
                                "CREATE TEMP TABLE staging_data%d AS SELECT %s FROM generate_series(1, %d) AS t(x);",
                                threadNum,
                                targetList,
                                numRowsPerFile));
                String filePath = String.format(
                        "%s/%s.db/%s/%s.parquet",
                        warehouseLocation,
                        dbName,
                        tableNames.get(j),
                        UUID.randomUUID());

                stmt.execute(
                        String.format(
                                Locale.getDefault(),
                                "COPY staging_data%d TO '%s' (FORMAT PARQUET);",
                                threadNum,
                                filePath));
                stmt.execute(String.format(Locale.getDefault(), "DROP TABLE staging_data%d;", threadNum));

                filePaths.add(filePath);
            }

            for (int j = 0; j < tableObjects.size(); j++) {
                // Append files to Raven Catalog
                List<FileObject> newDataFiles = Lists.newArrayList();
                FileObject newFileObject = FileObject.newBuilder()
                        .setTag("newdata")
                        .setPath(filePaths.get(j))
                        .build();
                newDataFiles.add(newFileObject);

                ravenCatalog.finalAppendFiles(tableObjects.get(j), newDataFiles, txnId);
            }

            boolean success = ravenCatalog.commit(txnId);

            if (!success) {
                deleteFiles(filePaths);
            }

            Instant afterTxn = Instant.now();

            // Record end-to-end metrics
            TXN_TIMES.get(threadNum).add(new TimePair(beforeTxn, afterTxn));
            OP_SUCCESS.get(threadNum).add(success);

        } catch (Exception e) {
            LOG.info("Database error occurred during Raven insert", e);
        }
    }

    public static void runTaskForDuration(BiConsumer<AtomicBoolean, Integer> task, long duration, TimeUnit unit) {
        AtomicBoolean running = new AtomicBoolean(true);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        for (int i = 0; i < numThreads; i++) {
            Integer threadId = i;
            executor.execute(() -> task.accept(running, threadId));
        }

        @SuppressWarnings("unused")
        Future<?> unused = scheduler.schedule(() -> {
            running.set(false);
            scheduler.shutdown();
        }, duration, unit);

        executor.shutdown();
        try {
            if (!executor.awaitTermination(duration + 15, unit)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public static void deleteFiles(List<String> s3Paths) {
        if (s3Paths == null || s3Paths.isEmpty() || s3 == null) {
            return;
        }

        String bucket = URI.create(s3Paths.get(0)).getHost();
        List<ObjectIdentifier> allKeys = Lists.newArrayListWithCapacity(s3Paths.size());
        for (String path : s3Paths) {
            String key = URI.create(path).getPath().substring(1);
            allKeys.add(ObjectIdentifier.builder().key(key).build());
        }

        int chunkSize = 1000;
        for (int i = 0; i < allKeys.size(); i += chunkSize) {
            List<ObjectIdentifier> chunk = allKeys.subList(i, Math.min(i + chunkSize, allKeys.size()));

            Delete delete = Delete.builder()
                    .objects(chunk)
                    .quiet(true)
                    .build();

            DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                    .bucket(bucket)
                    .delete(delete)
                    .build();

            try {
                s3.deleteObjects(request);
            } catch (S3Exception e) {
                LOG.error("AWS API error during batch delete", e);
            }
        }
    }
}
