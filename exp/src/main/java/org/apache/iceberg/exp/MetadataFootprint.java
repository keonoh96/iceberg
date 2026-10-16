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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.File2;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hive.HiveCatalog2;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog2;
import org.apache.iceberg.raven.CatalogOuterClass.FileObject;
import org.apache.iceberg.raven.CatalogOuterClass.StartTransactionResponse;
import org.apache.iceberg.raven.CatalogOuterClass.TableObject;
import org.apache.iceberg.raven.RavenCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

/**
 * MetadataFootprint benchmarks the metadata storage footprint in PostgreSQL
 * for 100,000 files (1 row per file) on a single table with integer columns,
 * supporting both DuckLake and Raven catalogs.
 */
public class MetadataFootprint {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataFootprint.class);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    // Experiment configurations
    private static String mode;
    private static String catalogType;
    private static boolean isDuckLake;
    private static int numFiles = 100000;
    private static int numColumns = 10;
    private static int numMeasures = 100;
    private static int logInterval = 10000;
    private static final String FIRST_COL_NAME = "col1";

    private static String workspaceName;
    private static String dbName;
    private static String tableName;
    private static String warehouseLocation;
    private static String ducklakeDataPath;

    // PostgreSQL connection parameters (shared by both DuckLake and Raven)
    private static String pgHost;
    private static String pgPort;
    private static String pgDatabase;
    private static String pgUser;
    private static String pgPassword;

    // S3 configuration
    private static String s3Secret;
    private static String s3KeyId;
    private static String s3Region;
    private static String s3Endpoint;
    private static S3Client s3;

    // Raven / Hive configuration
    private static String ravenAddress;
    private static String hmsUri;
    private static HiveCatalog2 catalog2;
    private static RavenCatalog ravenCatalog;

    public static class ReadMeasurement {
        public final int iteration;
        public final int targetValue;
        public final long latencyMs;
        public final Instant start;
        public final Instant end;

        public ReadMeasurement(int iteration, int targetValue, long latencyMs, Instant start, Instant end) {
            this.iteration = iteration;
            this.targetValue = targetValue;
            this.latencyMs = latencyMs;
            this.start = start;
            this.end = end;
        }
    }

    private MetadataFootprint() {}

    public static Map<String, String> parseJsonToMap(String jsonFilePath) throws IOException {
        File file = new File(jsonFilePath);
        return JSON_MAPPER.readValue(file, new TypeReference<Map<String, String>>() {});
    }

    private static void loadConfigs(Map<String, String> expConfigs) {
        // Mode: "populate" (default) or "read" (measure read performance only)
        String modeConfig = expConfigs.getOrDefault("mode",
                expConfigs.getOrDefault("operation",
                expConfigs.getOrDefault("operation_type",
                expConfigs.getOrDefault("op_type",
                expConfigs.getOrDefault("run_mode",
                expConfigs.getOrDefault("action", "populate"))))));

        if (modeConfig.equalsIgnoreCase("read")
                || modeConfig.equalsIgnoreCase("read_only")
                || modeConfig.equalsIgnoreCase("measure_read")
                || "true".equalsIgnoreCase(expConfigs.get("measure_read"))
                || "true".equalsIgnoreCase(expConfigs.get("read_only"))
                || "false".equalsIgnoreCase(expConfigs.get("populate"))) {
            mode = "read";
        } else {
            mode = "populate";
        }

        // Catalog type: ducklake vs raven
        String catalogTypeConfig = expConfigs.getOrDefault("catalog_type", expConfigs.get("exp_type"));
        if (catalogTypeConfig != null) {
            catalogType = catalogTypeConfig.toLowerCase(Locale.getDefault());
            isDuckLake = catalogType.contains("ducklake");
        } else {
            String isDuckLakeStr = expConfigs.get("is_duck_lake");
            isDuckLake = isDuckLakeStr != null && (isDuckLakeStr.equalsIgnoreCase("true") || isDuckLakeStr.equals("1"));
            catalogType = isDuckLake ? "ducklake" : "raven";
        }

        // Number of files (default: 100,000)
        String numFilesStr = expConfigs.getOrDefault("num_files", expConfigs.get("num_rows"));
        if (numFilesStr != null && !numFilesStr.isEmpty() && !numFilesStr.equalsIgnoreCase("default")) {
            numFiles = Integer.parseInt(numFilesStr);
        } else {
            numFiles = 100000;
        }

        // Number of columns (default: 10)
        String numColsStr = expConfigs.getOrDefault("num_columns", expConfigs.get("columns"));
        if (numColsStr != null && !numColsStr.isEmpty() && !numColsStr.equalsIgnoreCase("default")) {
            numColumns = Integer.parseInt(numColsStr);
        } else {
            numColumns = 10;
        }

        // Number of read measurements (default: 100)
        String numMeasuresStr = expConfigs.getOrDefault("num_measures",
                expConfigs.getOrDefault("num_reads", expConfigs.get("num_measurements")));
        if (numMeasuresStr != null && !numMeasuresStr.isEmpty() && !numMeasuresStr.equalsIgnoreCase("default")) {
            numMeasures = Integer.parseInt(numMeasuresStr);
        } else {
            numMeasures = 100;
        }

        String logIntervalStr = expConfigs.get("log_interval");
        if (logIntervalStr != null && !logIntervalStr.isEmpty() && !logIntervalStr.equalsIgnoreCase("default")) {
            logInterval = Integer.parseInt(logIntervalStr);
        } else if (numFiles < 10000) {
            logInterval = Math.max(1, numFiles / 10);
        } else {
            logInterval = 10000;
        }

        workspaceName = expConfigs.getOrDefault("workspace_name", "public");
        dbName = expConfigs.getOrDefault("db_name", "retail");
        tableName = expConfigs.getOrDefault("table_name", "table1");
        warehouseLocation = expConfigs.getOrDefault("warehouse_location", "s3://umdraven/warehouse");
        ducklakeDataPath = expConfigs.getOrDefault("ducklake_data_path", warehouseLocation);

        // PostgreSQL configs (same names for both DuckLake and Raven)
        pgHost = expConfigs.getOrDefault("pg_host", expConfigs.getOrDefault("db_host", "127.0.0.1"));
        pgPort = expConfigs.getOrDefault("pg_port", expConfigs.getOrDefault("db_port", "5432"));
        pgDatabase = expConfigs.getOrDefault(
                "pg_database",
                expConfigs.getOrDefault("pg_db_name", expConfigs.getOrDefault("db_name", "ducklakedb")));
        pgUser = expConfigs.getOrDefault("pg_user", expConfigs.getOrDefault("db_user", "postgres"));
        pgPassword = expConfigs.getOrDefault("pg_password", expConfigs.getOrDefault("db_password", ""));

        // S3 credentials
        s3Secret = expConfigs.getOrDefault("s3_secret", "");
        s3KeyId = expConfigs.getOrDefault("s3_key_id", "");
        s3Region = expConfigs.getOrDefault("s3_region", "us-east-1");
        s3Endpoint = expConfigs.get("s3_endpoint");
        if (s3Endpoint != null && s3Endpoint.equalsIgnoreCase("default")) {
            s3Endpoint = null;
        }

        // Raven / HMS
        ravenAddress = expConfigs.getOrDefault("raven_address", "127.0.0.1:9876");
        String hmsUriConfig = expConfigs.getOrDefault("hms_uri", expConfigs.get("thrift_uri"));
        if (hmsUriConfig != null && !hmsUriConfig.isEmpty() && !hmsUriConfig.equalsIgnoreCase("default")) {
            hmsUri = hmsUriConfig;
        } else {
            hmsUri = "thrift://localhost:9083";
        }
    }

    private static Schema createSchema(int numCols) {
        List<Types.NestedField> fields = Lists.newArrayListWithCapacity(numCols);
        for (int i = 1; i <= numCols; i++) {
            fields.add(Types.NestedField.required(i, "col" + i, Types.IntegerType.get()));
        }
        return new Schema(fields);
    }

    private static String schemaToTargetList(Schema schema) {
        List<Types.NestedField> columns = schema.columns();
        StringBuilder sb = new StringBuilder();
        for (Types.NestedField column : columns) {
            sb.append(String.format(Locale.getDefault(), "x::INTEGER AS %s,", column.name()));
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    private static Metrics computeStats(Schema schema, int i) {
        Map<Integer, Long> valueCounts = Maps.newHashMap();
        Map<Integer, Long> nullValueCounts = Maps.newHashMap();
        Map<Integer, Long> nanValueCounts = Maps.newHashMap();
        Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
        Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();

        ByteBuffer valBytes = ByteBuffer.allocate(Integer.BYTES);
        valBytes.order(ByteOrder.LITTLE_ENDIAN);
        valBytes.putInt(i + 1);
        valBytes.rewind();

        List<Types.NestedField> columns = schema.columns();
        for (Types.NestedField column : columns) {
            valueCounts.put(column.fieldId(), 1L);
            nullValueCounts.put(column.fieldId(), 0L);
            nanValueCounts.put(column.fieldId(), 0L);
            lowerBounds.put(column.fieldId(), valBytes.duplicate());
            upperBounds.put(column.fieldId(), valBytes.duplicate());
        }

        return new Metrics(1L, null, valueCounts, nullValueCounts, nanValueCounts, lowerBounds, upperBounds);
    }

    private static void executeSql(DuckDBConnection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static DuckDBConnection initDuckDB() throws SQLException {
        DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
        executeSql(conn, "INSTALL httpfs; LOAD httpfs;");
        executeSql(conn, "INSTALL parquet; LOAD parquet;");

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
        executeSql(conn, secretSql);

        if (isDuckLake) {
            try {
                executeSql(conn, "INSTALL postgres_scanner; LOAD postgres_scanner;");
            } catch (SQLException e) {
                LOG.warn("Failed to load postgres_scanner, trying postgres", e);
                executeSql(conn, "INSTALL postgres; LOAD postgres;");
            }

            try {
                executeSql(conn, "INSTALL ducklake; LOAD ducklake;");
            } catch (SQLException e) {
                LOG.warn("Failed to INSTALL/LOAD ducklake, trying core_nightly", e);
                executeSql(conn, "FORCE INSTALL ducklake FROM core_nightly; LOAD ducklake;");
            }

            StringBuilder connParams = new StringBuilder();
            connParams.append(String.format("dbname=%s host=%s port=%s user=%s", pgDatabase, pgHost, pgPort, pgUser));
            if (pgPassword != null && !pgPassword.isEmpty()) {
                connParams.append(" password=").append(pgPassword);
            }

            String attachSql = String.format(
                    Locale.getDefault(),
                    "ATTACH 'ducklake:postgres:%s' AS ducklake (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT 0);",
                    connParams.toString(),
                    ducklakeDataPath);
            executeSql(conn, attachSql);

            try {
                executeSql(conn, "CALL ducklake.set_option('data_inlining_row_limit', 0);");
            } catch (SQLException e) {
                LOG.debug("Setting option via CALL ducklake.set_option was not needed or not supported", e);
            }

            try {
                executeSql(conn, "USE ducklake;");
            } catch (SQLException e) {
                LOG.debug("Setting default schema to ducklake failed", e);
            }
        }
        return conn;
    }

    private static void initRavenCatalog() {
        Map<String, String> conf = Maps.newHashMap();
        conf.put(CatalogProperties.URI, hmsUri);
        conf.put(
                CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                String.valueOf(TimeUnit.SECONDS.toMillis(10)));
        conf.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
        conf.put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName());
        conf.put(S3FileIOProperties.ACCESS_KEY_ID, s3KeyId);
        conf.put(S3FileIOProperties.SECRET_ACCESS_KEY, s3Secret);
        conf.put(AwsClientProperties.CLIENT_REGION, s3Region);
        if (s3Endpoint != null && !s3Endpoint.isEmpty()) {
            conf.put(S3FileIOProperties.ENDPOINT, s3Endpoint);
        }
        conf.put("raven_address", ravenAddress);
        conf.put("workspace_name", workspaceName);

        AwsClientFactories.defaultFactory().initialize(conf);
        s3 = AwsClientFactories.defaultFactory().s3();

        HiveConf hiveConf = new HiveConf();
        catalog2 = (HiveCatalog2) CatalogUtil.loadCatalog(
                HiveCatalog2.class.getName(),
                CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
                conf,
                hiveConf);
    }

    private static void createDuckLakeTable(DuckDBConnection conn) throws SQLException {
        StringBuilder colDefs = new StringBuilder();
        for (int i = 1; i <= numColumns; i++) {
            colDefs.append("col").append(i).append(" INTEGER");
            if (i < numColumns) {
                colDefs.append(", ");
            }
        }
        String createSql = String.format(
                Locale.getDefault(),
                "CREATE TABLE IF NOT EXISTS ducklake.%s (%s);",
                tableName,
                colDefs.toString());
        LOG.info("Creating DuckLake table: {}", createSql);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createSql);
        }
    }

    private static void runDuckLake(DuckDBConnection conn, String targetList) throws SQLException {
        LOG.info("Starting DuckLake footprint insertion: {} files, {} columns, table '{}'...",
                numFiles, numColumns, tableName);
        createDuckLakeTable(conn);

        long startTime = System.currentTimeMillis();
        try (Statement stmt = conn.createStatement()) {
            for (int i = 0; i < numFiles; i++) {
                String insertSql = String.format(
                        Locale.getDefault(),
                        "INSERT INTO ducklake.%s SELECT %s FROM generate_series(%d, %d) AS t(x);",
                        tableName,
                        targetList,
                        i + 1,
                        i + 1);
                stmt.execute(insertSql);

                if ((i + 1) % logInterval == 0 || i + 1 == numFiles) {
                    double elapsed = (System.currentTimeMillis() - startTime) / 1000.0;
                    double rate = (i + 1) / (elapsed > 0 ? elapsed : 1.0);
                    double eta = rate > 0 ? (numFiles - (i + 1)) / rate : 0;
                    LOG.info("  -> Inserted {}/{} files ({:.1f}%) | Rate: {:.1f} files/s | Elapsed: {:.1f}s | ETA: {:.1f}s",
                            i + 1, numFiles, ((i + 1) * 100.0) / numFiles, rate, elapsed, eta);
                }
            }
        }
        LOG.info("DuckLake insertion completed successfully.");
    }

    private static void runRaven(
            DuckDBConnection duckDbConn,
            TableIdentifier tableIdent,
            Schema schema,
            PartitionSpec spec,
            String targetList) throws SQLException {
        LOG.info("Starting Raven footprint insertion: {} files, {} columns, table '{}.{}'...",
                numFiles, numColumns, dbName, tableName);

        initRavenCatalog();
        String location = String.format("%s/%s.db/%s", warehouseLocation, dbName, tableName);

        try {
            catalog2.createNamespace(Namespace.of(dbName));
        } catch (Exception e) {
            LOG.debug("Namespace may already exist", e);
        }

        if (!catalog2.tableExists(tableIdent)) {
            catalog2.createTable(tableIdent, schema, spec, location, ImmutableMap.of());
        }

        ravenCatalog = new RavenCatalog(ravenAddress);

        long startTime = System.currentTimeMillis();

        try (Statement stmt = duckDbConn.createStatement()) {
            for (int i = 0; i < numFiles; i++) {
                // 1. Start Raven transaction
                StartTransactionResponse ravenTxn = ravenCatalog.startTransaction(
                        workspaceName, dbName, Lists.newArrayList(tableName));
                long txnId = ravenTxn.getTxnId();
                TableObject tableObject = ravenTxn.getTablesList().get(0);

                // 2. Start Iceberg transaction
                Table table = catalog2.loadTable(tableIdent);
                AppendFiles txn = table.newFastAppend();

                // 3. Write data file using DuckDB (1 row per file)
                stmt.execute(
                        String.format(
                                Locale.getDefault(),
                                "CREATE TEMP TABLE staging_data AS SELECT %s FROM generate_series(%d, %d) AS t(x);",
                                targetList,
                                i + 1,
                                i + 1));

                String filePath = String.format("%s/%s.parquet", table.location(), UUID.randomUUID());
                stmt.execute(
                        String.format(
                                Locale.getDefault(), "COPY staging_data TO '%s' (FORMAT PARQUET);", filePath));
                stmt.execute("DROP TABLE staging_data;");

                long fileSize = getFileSize(filePath);

                // 4. Construct file metrics and commit to Iceberg
                Metrics metrics = computeStats(schema, i);
                DataFile newFile = DataFiles.builder(spec)
                        .withFileSizeInBytes(fileSize)
                        .withFormat(FileFormat.PARQUET)
                        .withMetrics(metrics)
                        .withPath(filePath)
                        .build();

                txn.appendFile(newFile);

                List<File2> fileLogs = Lists.newArrayList();
                fileLogs.add(new File2(filePath, File2.File2Type.ADD, "data"));

                txn.commit2(fileLogs);

                // 5. Commit to Raven (including the new data and metadata files)
                List<FileObject> newDataFiles = Lists.newArrayList();
                List<String> filesToReplace = Lists.newArrayList();

                for (File2 file : fileLogs) {
                    switch (file.fileType()) {
                        case ADD:
                            FileObject newMetadataFileObject = FileObject.newBuilder()
                                    .setTag(file.tag()).setPath(file.path()).build();
                            newDataFiles.add(newMetadataFileObject);
                            break;
                        case DELETE:
                            filesToReplace.add(file.path());
                            break;
                        default:
                            break;
                    }
                }

                ravenCatalog.finalRewriteFiles(tableObject, filesToReplace, newDataFiles, txnId);
                boolean success = ravenCatalog.commit(txnId);
                if (!success) {
                    LOG.warn("Raven transaction commit returned false for fileIdx {}", i);
                }

                if ((i + 1) % logInterval == 0 || i + 1 == numFiles) {
                    double elapsed = (System.currentTimeMillis() - startTime) / 1000.0;
                    double rate = (i + 1) / (elapsed > 0 ? elapsed : 1.0);
                    double eta = rate > 0 ? (numFiles - (i + 1)) / rate : 0;
                    LOG.info("  -> Inserted {}/{} files ({:.1f}%) | Rate: {:.1f} files/s | Elapsed: {:.1f}s | ETA: {:.1f}s",
                            i + 1, numFiles, ((i + 1) * 100.0) / numFiles, rate, elapsed, eta);
                }
            }
        }
        LOG.info("Raven insertion completed successfully.");
    }

    private static long getFileSize(String path) {
        URI uri = URI.create(path);
        String bucket = uri.getHost();
        String key = uri.getPath().substring(1);
        HeadObjectRequest headRequest = HeadObjectRequest.builder().bucket(bucket).key(key).build();
        return s3.headObject(headRequest).contentLength();
    }

    public static long measurePostgresStorageBytes(
            String host, String port, String database, String user, String password) throws SQLException {
        String url = String.format(Locale.getDefault(), "jdbc:postgresql://%s:%s/%s", host, port, database);
        Properties props = new Properties();
        props.setProperty("user", user);
        if (password != null && !password.isEmpty()) {
            props.setProperty("password", password);
        }

        try (Connection conn = DriverManager.getConnection(url, props);
             Statement stmt = conn.createStatement()) {

            long dbSize = -1;
            try (ResultSet rs = stmt.executeQuery(
                    String.format(Locale.getDefault(), "SELECT pg_database_size('%s');", database))) {
                if (rs.next()) {
                    dbSize = rs.getLong(1);
                }
            }

            // Log table-level storage breakdown
            LOG.info("PostgreSQL storage size breakdown for database '{}':", database);
            String tableBreakdownSql =
                    "SELECT relname AS table_name, " +
                    "       pg_total_relation_size(c.oid) AS total_bytes, " +
                    "       pg_size_pretty(pg_total_relation_size(c.oid)) AS pretty_size " +
                    "FROM pg_class c " +
                    "JOIN pg_namespace n ON n.oid = c.relnamespace " +
                    "WHERE nspname NOT IN ('pg_catalog', 'information_schema') " +
                    "ORDER BY pg_total_relation_size(c.oid) DESC LIMIT 20;";

            try (ResultSet rs = stmt.executeQuery(tableBreakdownSql)) {
                while (rs.next()) {
                    LOG.info("  Table {}: {} bytes ({})",
                            rs.getString("table_name"),
                            rs.getLong("total_bytes"),
                            rs.getString("pretty_size"));
                }
            } catch (SQLException e) {
                LOG.debug("Could not fetch table breakdown", e);
            }

            return dbSize;
        }
    }

    private static String formatBytes(long bytes) {
        if (bytes < 0) {
            return "N/A";
        }
        if (bytes < 1024) {
            return bytes + " B";
        }
        int exp = (int) (Math.log((double) bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp - 1) + "B";
        return String.format(Locale.getDefault(), "%.1f %s", (double) bytes / Math.pow(1024, exp), pre);
    }

    private static void writeResults(
            Map<String, String> expConfigs,
            long postgresStorageBytes,
            double elapsedSeconds) throws IOException {

        String prettySize = formatBytes(postgresStorageBytes);

        LOG.info("================================================================================");
        LOG.info(" Metadata Footprint Benchmark Result:");
        LOG.info("   Catalog Type:             {}", catalogType);
        LOG.info("   Files Written:            {}", numFiles);
        LOG.info("   Columns:                  {}", numColumns);
        LOG.info("   PostgreSQL Database:      {}", pgDatabase);
        LOG.info("   Database Storage Size:    {} bytes ({})", postgresStorageBytes, prettySize);
        LOG.info("   Elapsed Time:             {:.2f} s", elapsedSeconds);
        LOG.info("================================================================================");

        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        ObjectNode resultNode = mapper.createObjectNode();
        resultNode.put("exp_name", "MetadataFootprint");
        resultNode.put("mode", mode);
        resultNode.put("catalog_type", catalogType);
        resultNode.put("num_files", numFiles);
        resultNode.put("num_columns", numColumns);
        resultNode.put("pg_database", pgDatabase);
        resultNode.put("pg_host", pgHost);
        resultNode.put("pg_port", pgPort);
        resultNode.put("postgres_storage_bytes", postgresStorageBytes);
        resultNode.put("human_readable_size", prettySize);
        resultNode.put("elapsed_seconds", elapsedSeconds);

        // Include any non-secret configs in the JSON
        ObjectNode configsNode = mapper.createObjectNode();
        for (Map.Entry<String, String> entry : expConfigs.entrySet()) {
            String k = entry.getKey().toLowerCase(Locale.getDefault());
            if (!k.contains("secret") && !k.contains("password")) {
                configsNode.put(entry.getKey(), entry.getValue());
            }
        }
        resultNode.set("configs", configsNode);

        String outputFile = expConfigs.get("output_file");
        String expResultDir = expConfigs.get("exp_result_dir");

        if (outputFile != null && !outputFile.isEmpty()) {
            Path path = Paths.get(outputFile);
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            if (outputFile.endsWith(".json")) {
                try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
                    writer.write(mapper.writeValueAsString(resultNode));
                    writer.newLine();
                }
            } else {
                try (BufferedWriter writer = Files.newBufferedWriter(
                        path,
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND)) {
                    writer.write(String.format(
                            Locale.getDefault(),
                            "%d,%d,%s,%d,%s\n",
                            numColumns,
                            numFiles,
                            catalogType,
                            postgresStorageBytes,
                            prettySize));
                }
            }
            LOG.info("Result written to: {}", outputFile);
        }

        if (expResultDir != null && !expResultDir.isEmpty()) {
            Path dir = Paths.get(expResultDir);
            Files.createDirectories(dir);

            // 1. Write JSON summary
            Path jsonPath = dir.resolve(String.format(
                    Locale.getDefault(),
                    "footprint-%s-%dcols-%dfiles.json",
                    catalogType,
                    numColumns,
                    numFiles));
            try (BufferedWriter writer = Files.newBufferedWriter(jsonPath, StandardCharsets.UTF_8)) {
                writer.write(mapper.writeValueAsString(resultNode));
                writer.newLine();
            }

            // 2. Append to footprint_result.txt
            Path txtPath = dir.resolve("footprint_result.txt");
            boolean writeHeader = !Files.exists(txtPath);
            try (BufferedWriter writer = Files.newBufferedWriter(
                    txtPath,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND)) {
                if (writeHeader) {
                    writer.write("columns,files,catalog_type,postgres_data_storage_bytes,human_readable_size\n");
                }
                writer.write(String.format(
                        Locale.getDefault(),
                        "%d,%d,%s,%d,%s\n",
                        numColumns,
                        numFiles,
                        catalogType,
                        postgresStorageBytes,
                        prettySize));
            }
            LOG.info("Results saved in result directory: {}", expResultDir);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            LOG.error("Usage: MetadataFootprint <config.json>");
            return;
        }

        Map<String, String> expConfigs = parseJsonToMap(args[0]);
        loadConfigs(expConfigs);

        Schema schema = createSchema(numColumns);
        PartitionSpec spec = PartitionSpec.builderFor(schema).build();
        String targetList = schemaToTargetList(schema);

        try (DuckDBConnection duckDbConn = initDuckDB()) {
            TableIdentifier tableIdent = TableIdentifier.of(dbName, tableName);

            if (!isDuckLake) {
                initRavenCatalog();
                ravenCatalog = new RavenCatalog(ravenAddress);
            }

            if ("read".equalsIgnoreCase(mode)) {
                // Operation 2: Measure read performance only
                LOG.info("Running MetadataFootprint in READ mode: executing {} read measurements...",
                        numMeasures);
                List<ReadMeasurement> readMeasurements;
                if (isDuckLake) {
                    readMeasurements = testReadPerformanceDuckLake(duckDbConn, numMeasures, numFiles, FIRST_COL_NAME);
                } else {
                    readMeasurements = testReadPerformanceRaven(duckDbConn, tableIdent, numMeasures, numFiles, FIRST_COL_NAME);
                }
                writeReadResults(expConfigs, readMeasurements, catalogType, numColumns, numFiles);
            } else {
                // Operation 1: Populate database only (default)
                LOG.info("Running MetadataFootprint in POPULATE mode (default): inserting {} files and measuring footprint...",
                        numFiles);
                long startTimeMs = System.currentTimeMillis();

                if (isDuckLake) {
                    runDuckLake(duckDbConn, targetList);
                } else {
                    runRaven(duckDbConn, tableIdent, schema, spec, targetList);
                }

                long elapsedMs = System.currentTimeMillis() - startTimeMs;
                double elapsedSeconds = elapsedMs / 1000.0;

                // Measure total data storage footprint in PostgreSQL
                LOG.info("Connecting to PostgreSQL at {}:{} (database: {}) to measure footprint...",
                        pgHost, pgPort, pgDatabase);
                long storageBytes = -1;
                try {
                    storageBytes = measurePostgresStorageBytes(pgHost, pgPort, pgDatabase, pgUser, pgPassword);
                } catch (SQLException e) {
                    LOG.error("Failed to measure PostgreSQL storage footprint", e);
                }

                // Write output for footprint
                writeResults(expConfigs, storageBytes, elapsedSeconds);
            }
        }
    }

    /**
     * Tests read performance on DuckLake by selecting full row filtered on col1 = targetVal.
     */
    private static List<ReadMeasurement> testReadPerformanceDuckLake(
            DuckDBConnection conn, int measures, int totalFiles, String firstColName) throws SQLException {
        LOG.info("Testing read performance for DuckLake: {} measurements across {} files...",
                measures, totalFiles);
        List<ReadMeasurement> measurements = Lists.newArrayListWithCapacity(measures);

        try (Statement stmt = conn.createStatement()) {
            for (int i = 0; i < measures; i++) {
                int targetVal = ThreadLocalRandom.current().nextInt(1, totalFiles + 1);

                Instant start = Instant.now();
                String query = String.format(
                        Locale.getDefault(),
                        "SELECT * FROM ducklake.%s WHERE %s = %d;",
                        tableName,
                        firstColName,
                        targetVal);

                try (ResultSet rs = stmt.executeQuery(query)) {
                    while (rs.next()) {
                        for (int col = 1; col <= numColumns; col++) {
                            rs.getInt(col);
                        }
                    }
                }
                Instant end = Instant.now();

                long latencyMs = Duration.between(start, end).toMillis();
                measurements.add(new ReadMeasurement(i + 1, targetVal, latencyMs, start, end));

                if ((i + 1) % Math.max(1, measures / 10) == 0 || i + 1 == measures) {
                    LOG.info("  -> Read {}/{} measurements completed (latest latency: {} ms)",
                            i + 1, measures, latencyMs);
                }
            }
        }
        return measurements;
    }

    /**
     * Tests read performance on Raven by planning files with Iceberg metrics filter on col1 = targetVal
     * and scanning the matching files with DuckDB.
     */
    private static List<ReadMeasurement> testReadPerformanceRaven(
            DuckDBConnection duckDbConn,
            TableIdentifier tableIdent,
            int measures,
            int totalFiles,
            String firstColName) throws SQLException {
        LOG.info("Testing read performance for Raven: {} measurements across {} files...",
                measures, totalFiles);
        List<ReadMeasurement> measurements = Lists.newArrayListWithCapacity(measures);

        try (Statement stmt = duckDbConn.createStatement()) {
            for (int i = 0; i < measures; i++) {
                int targetVal = ThreadLocalRandom.current().nextInt(1, totalFiles + 1);

                Instant start = Instant.now();

                // 1. Load table metadata via Raven / Hive catalog
                Table table = catalog2.loadTable(tableIdent);

                // 2. Plan files using Iceberg filter on the first column
                List<String> filesToScan = Lists.newArrayList();
                try (CloseableIterable<FileScanTask> tasks = table.newScan()
                        .filter(Expressions.equal(firstColName, targetVal))
                        .planFiles()) {
                    for (FileScanTask task : tasks) {
                        filesToScan.add(task.file().location());
                    }
                } catch (IOException e) {
                    LOG.error("Failed to plan files for Raven read query", e);
                }

                // 3. Scan the matching files using DuckDB
                if (!filesToScan.isEmpty()) {
                    String filesStr = filesToScan.stream()
                            .map(s -> "'" + s + "'")
                            .collect(Collectors.joining(","));
                    String query = String.format(
                            Locale.getDefault(),
                            "SELECT * FROM read_parquet([ %s ]) WHERE %s = %d;",
                            filesStr,
                            firstColName,
                            targetVal);
                    try (ResultSet rs = stmt.executeQuery(query)) {
                        while (rs.next()) {
                            for (int col = 1; col <= numColumns; col++) {
                                rs.getInt(col);
                            }
                        }
                    }
                }

                Instant end = Instant.now();

                long latencyMs = Duration.between(start, end).toMillis();
                measurements.add(new ReadMeasurement(i + 1, targetVal, latencyMs, start, end));

                if ((i + 1) % Math.max(1, measures / 10) == 0 || i + 1 == measures) {
                    LOG.info("  -> Read {}/{} measurements completed (latest latency: {} ms)",
                            i + 1, measures, latencyMs);
                }
            }
        }
        return measurements;
    }

    /**
     * Writes read benchmark measurements and latency percentiles to a separate output file.
     */
    private static void writeReadResults(
            Map<String, String> expConfigs,
            List<ReadMeasurement> measurements,
            String catalogType,
            int numCols,
            int totalFiles) throws IOException {

        if (measurements == null || measurements.isEmpty()) {
            return;
        }

        List<Long> latencies = measurements.stream()
                .map(m -> m.latencyMs)
                .sorted()
                .collect(Collectors.toList());

        long sum = 0;
        for (long l : latencies) {
            sum += l;
        }
        double avg = (double) sum / (double) latencies.size();
        long min = latencies.get(0);
        long max = latencies.get(latencies.size() - 1);
        long p50 = latencies.get((int) ((double) latencies.size() * 0.50));
        long p95 = latencies.get((int) ((double) latencies.size() * 0.95));
        long p99 = latencies.get(Math.min(latencies.size() - 1, (int) ((double) latencies.size() * 0.99)));

        LOG.info("================================================================================");
        LOG.info(" Read Performance Benchmark Result ({}):", catalogType);
        LOG.info("   Measurements:             {}", latencies.size());
        LOG.info("   Average Latency:          {:.2f} ms", avg);
        LOG.info("   Min / Max Latency:        {} ms / {} ms", min, max);
        LOG.info("   P50 / P95 / P99:          {} ms / {} ms / {} ms", p50, p95, p99);
        LOG.info("================================================================================");

        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        String readOutputFile = expConfigs.get("read_output_file");
        String expResultDir = expConfigs.get("exp_result_dir");

        // Helper consumer to write NDJSON entries (1 row per measurement, no miscellaneous outer wrappers)
        Path targetLogPath = null;
        if (readOutputFile != null && !readOutputFile.isEmpty()) {
            targetLogPath = Paths.get(readOutputFile);
        } else if (expConfigs.containsKey("output_file")) {
            String outputFile = expConfigs.get("output_file");
            Path parent = Paths.get(outputFile).getParent();
            String fileName = Paths.get(outputFile).getFileName().toString();
            String readFileName = fileName.replace("footprint", "read-latency");
            if (readFileName.equals(fileName)) {
                readFileName = "read-latency-" + fileName;
            }
            targetLogPath = parent != null ? parent.resolve(readFileName) : Paths.get(readFileName);
        } else if (expResultDir != null && !expResultDir.isEmpty()) {
            Path dir = Paths.get(expResultDir);
            targetLogPath = dir.resolve(String.format(
                    Locale.getDefault(),
                    "read-latency-%s-%dcols-%dfiles.json",
                    catalogType,
                    numCols,
                    totalFiles));
        }

        if (targetLogPath != null) {
            if (targetLogPath.getParent() != null) {
                Files.createDirectories(targetLogPath.getParent());
            }
            try (BufferedWriter writer = Files.newBufferedWriter(targetLogPath, StandardCharsets.UTF_8)) {
                for (ReadMeasurement m : measurements) {
                    ObjectNode row = mapper.createObjectNode();
                    row.put("iteration", m.iteration);
                    row.put("target_value", m.targetValue);
                    row.put("latency_ms", m.latencyMs);
                    row.put("catalog_type", catalogType);
                    row.put("num_columns", numCols);
                    row.put("num_files", totalFiles);
                    row.put("start", m.start.toString());
                    row.put("end", m.end.toString());

                    writer.write(mapper.writeValueAsString(row));
                    writer.newLine(); // Strictly NDJSON: one line per record for pandas.read_json(..., lines=True)
                }
            }
            LOG.info("Read measurement NDJSON written to: {}", targetLogPath);
        }

        if (expResultDir != null && !expResultDir.isEmpty()) {
            Path dir = Paths.get(expResultDir);
            Files.createDirectories(dir);

            // Append summary to summary.json
            Path summaryJsonPath = dir.resolve("summary.json");
            ObjectNode summaryNode = mapper.createObjectNode();
            summaryNode.put("exp_name", "MetadataFootprintRead");
            summaryNode.put("catalog_type", catalogType);
            summaryNode.put("num_files", totalFiles);
            summaryNode.put("num_columns", numCols);
            summaryNode.put("num_measures", latencies.size());
            summaryNode.put("avg_latency_ms", avg);
            summaryNode.put("min_latency_ms", min);
            summaryNode.put("max_latency_ms", max);
            summaryNode.put("p50_latency_ms", p50);
            summaryNode.put("p95_latency_ms", p95);
            summaryNode.put("p99_latency_ms", p99);

            try (BufferedWriter writer = Files.newBufferedWriter(
                    summaryJsonPath,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND)) {
                writer.write(mapper.writeValueAsString(summaryNode));
                writer.newLine();
            }

            // Append to summary text file
            Path txtPath = dir.resolve("read_result.txt");
            boolean writeHeader = !Files.exists(txtPath);
            try (BufferedWriter writer = Files.newBufferedWriter(
                    txtPath,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND)) {
                if (writeHeader) {
                    writer.write("columns,files,catalog_type,num_measures,avg_latency_ms,min_ms,max_ms,p50_ms,p95_ms,p99_ms\n");
                }
                writer.write(String.format(
                        Locale.getDefault(),
                        "%d,%d,%s,%d,%.2f,%d,%d,%d,%d,%d\n",
                        numCols,
                        totalFiles,
                        catalogType,
                        latencies.size(),
                        avg,
                        min,
                        max,
                        p50,
                        p95,
                        p99));
            }
            LOG.info("Read summary saved in result directory: {}", expResultDir);
        }
    }
}
