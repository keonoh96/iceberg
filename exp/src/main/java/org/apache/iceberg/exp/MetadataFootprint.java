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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.File2;
import org.apache.iceberg.FileFormat;
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
    private static String catalogType;
    private static boolean isDuckLake;
    private static int numFiles = 100000;
    private static int numColumns = 10;
    private static int logInterval = 10000;

    private static String workspaceName;
    private static String dbName;
    private static String tableName;
    private static String warehouseLocation;
    private static String ducklakeDataPath;
    private static String ducklakeExtensionPath;

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

    private MetadataFootprint() {}

    public static Map<String, String> parseJsonToMap(String jsonFilePath) throws IOException {
        File file = new File(jsonFilePath);
        return JSON_MAPPER.readValue(file, new TypeReference<Map<String, String>>() {});
    }

    private static void loadConfigs(Map<String, String> expConfigs) {
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
        if (expConfigs.containsKey("num_files")) {
            numFiles = Integer.parseInt(expConfigs.get("num_files"));
        } else if (expConfigs.containsKey("num_rows")) {
            numFiles = Integer.parseInt(expConfigs.get("num_rows"));
        } else {
            numFiles = 100000;
        }

        // Number of columns (default: 10)
        if (expConfigs.containsKey("num_columns")) {
            numColumns = Integer.parseInt(expConfigs.get("num_columns"));
        } else if (expConfigs.containsKey("columns")) {
            numColumns = Integer.parseInt(expConfigs.get("columns"));
        } else {
            numColumns = 10;
        }

        if (numFiles < 10000) {
            logInterval = Math.max(1, numFiles / 10);
        } else if (expConfigs.containsKey("log_interval")) {
            logInterval = Integer.parseInt(expConfigs.get("log_interval"));
        }

        workspaceName = expConfigs.getOrDefault("workspace_name", "public");
        dbName = expConfigs.getOrDefault("db_name", "retail");
        tableName = expConfigs.getOrDefault("table_name", "table1");
        warehouseLocation = expConfigs.getOrDefault("warehouse_location", "s3://umdraven/warehouse");
        ducklakeDataPath = expConfigs.getOrDefault("ducklake_data_path", warehouseLocation);
        ducklakeExtensionPath = expConfigs.get("ducklake_extension_path");

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

        // Raven / HMS
        ravenAddress = expConfigs.getOrDefault("raven_address", "127.0.0.1:9876");
        hmsUri = expConfigs.getOrDefault("hms_uri", expConfigs.getOrDefault("thrift_uri", "thrift://localhost:9083"));
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

    private static DuckDBConnection initDuckDB() throws SQLException {
        DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("INSTALL httpfs; LOAD httpfs;");
            stmt.execute("INSTALL parquet; LOAD parquet;");

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
                try {
                    stmt.execute("INSTALL postgres_scanner; LOAD postgres_scanner;");
                } catch (SQLException e) {
                    LOG.warn("Failed to load postgres_scanner, trying postgres", e);
                    stmt.execute("INSTALL postgres; LOAD postgres;");
                }

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

                // Disable data inlining globally in DuckLake so each insert creates a separate Parquet file in S3
                stmt.execute("SET ducklake_default_data_inlining_row_limit = 0;");

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

    private static void runDuckLake(String targetList) throws SQLException {
        LOG.info("Starting DuckLake footprint run: {} files, {} columns, table '{}'...",
                numFiles, numColumns, tableName);
        try (DuckDBConnection conn = initDuckDB()) {
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
        }
        LOG.info("DuckLake run completed successfully.");
    }

    private static void runRaven(Schema schema, PartitionSpec spec, String targetList) throws SQLException {
        LOG.info("Starting Raven footprint run: {} files, {} columns, table '{}.{}'...",
                numFiles, numColumns, dbName, tableName);

        initRavenCatalog();
        TableIdentifier tableIdent = TableIdentifier.of(dbName, tableName);
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

        try (DuckDBConnection duckDbConn = initDuckDB()) {
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
        }
        LOG.info("Raven run completed successfully.");
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

        long startTimeMs = System.currentTimeMillis();

        if (isDuckLake) {
            runDuckLake(targetList);
        } else {
            runRaven(schema, spec, targetList);
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

        // Write output
        writeResults(expConfigs, storageBytes, elapsedSeconds);
    }
}
