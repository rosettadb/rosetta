package com.adataptivescale.rosetta.source.core;

import com.adaptivescale.rosetta.common.JDBCDriverProvider;
import com.adaptivescale.rosetta.common.JDBCUtils;
import com.adaptivescale.rosetta.common.helpers.ModuleLoader;
import com.adaptivescale.rosetta.common.models.Database;
import com.adaptivescale.rosetta.common.models.Table;
import com.adaptivescale.rosetta.common.models.View;
import com.adaptivescale.rosetta.common.models.input.Connection;
import com.adaptivescale.rosetta.common.types.RosettaModuleTypes;
import com.adataptivescale.rosetta.source.core.extractors.column.ColumnsExtractor;
import com.adataptivescale.rosetta.source.core.extractors.table.DefaultTablesExtractor;
import com.adataptivescale.rosetta.source.core.extractors.view.DefaultViewExtractor;
import com.adataptivescale.rosetta.source.core.interfaces.ColumnExtractor;
import com.adataptivescale.rosetta.source.core.interfaces.Generator;
import com.adataptivescale.rosetta.source.core.interfaces.TableExtractor;
import com.adataptivescale.rosetta.source.core.interfaces.ViewExtractor;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;

@Slf4j
public class DuckLakeGenerator implements Generator<Database, Connection> {
    private final JDBCDriverProvider driverProvider;

    public DuckLakeGenerator(JDBCDriverProvider driverProvider) {
        this.driverProvider = driverProvider;
    }

    @Override
    public Database generate(Connection connection) throws Exception {
        validateDuckLakeConfig(connection);

        String duckdbUrl = buildDuckDbUrl(connection); // MUST be in-memory or session db, never metadata db
        java.sql.Connection jdbc = openDuckDbConnection(duckdbUrl, connection);

        try {
            String catalog = setupDuckLake(jdbc, connection);

            // Ensure extractor connection config points to correct catalog/schema
            Connection duckdbConnection = createDuckDbConnection(connection, duckdbUrl, catalog);

            TableExtractor tableExtractor = loadDuckDbTableExtractor(duckdbConnection);
            ViewExtractor viewExtractor   = loadDuckDbViewExtractor(duckdbConnection);
            ColumnExtractor colExtractor  = loadDuckDbColumnExtractor(duckdbConnection);

            Collection<Table> allTables = listTablesFromDuckLakeMetadata(jdbc, catalog, duckdbConnection.getSchemaName());
            if (allTables.isEmpty()) {
                try {
                    allTables = (Collection<Table>) tableExtractor.extract(duckdbConnection, jdbc);
                    if (allTables.isEmpty()) {
                        allTables = listTablesFallback(jdbc, catalog, duckdbConnection.getSchemaName());
                    }
                } catch (Exception e) {
                    log.warn("Table extractor failed, falling back to information_schema: {}", e.getMessage());
                    allTables = listTablesFallback(jdbc, catalog, duckdbConnection.getSchemaName());
                }
            }

            Collection<Table> tables = filterDuckLakeMetadataTables(allTables);
            log.info("Extracted {} user tables from {}.{}", tables.size(), catalog, duckdbConnection.getSchemaName());

            colExtractor.extract(jdbc, tables);

            Collection<View> views;
            try {
                views = (Collection<View>) viewExtractor.extract(duckdbConnection, jdbc);
            } catch (Exception e) {
                log.warn("View extractor failed; continuing with empty view set: {}", e.getMessage());
                views = List.of();
            }
            log.info("Extracted {} views", views.size());
            colExtractor.extract(jdbc, views);

            Database database = new Database();
            database.setName("ducklake:" + catalog);
            database.setTables(tables);
            database.setViews(views);
            database.setDatabaseType(connection.getDbType());
            return database;
        } finally {
            try { jdbc.close(); } catch (SQLException ignored) {}
        }
    }

    @Override
    public Database validate(Connection connection) throws Exception {
        validateDuckLakeConfig(connection);

        String duckdbUrl = buildDuckDbUrl(connection);
        java.sql.Connection jdbc = openDuckDbConnection(duckdbUrl, connection);
        try {
            setupDuckLake(jdbc, connection);
            Database database = new Database();
            database.setName("ducklake:" + connection.getDatabaseName());
            return database;
        } finally {
            try { jdbc.close(); } catch (SQLException ignored) {}
        }
    }

    /** Allowed for catalog/schema identifiers to prevent SQL injection. */
    private static final java.util.regex.Pattern SAFE_IDENTIFIER = java.util.regex.Pattern.compile("^[a-zA-Z0-9_]+$");

    private void validateDuckLakeConfig(Connection c) {
        if (c.getDatabaseName() == null || c.getDatabaseName().isBlank()) {
            throw new IllegalArgumentException("databaseName is required for DuckLake connections");
        }
        if (!SAFE_IDENTIFIER.matcher(c.getDatabaseName()).matches()) {
            throw new IllegalArgumentException("databaseName must contain only alphanumeric characters and underscores");
        }
        String schema = c.getSchemaName();
        if (schema != null && !schema.isBlank() && !SAFE_IDENTIFIER.matcher(schema).matches()) {
            throw new IllegalArgumentException("schemaName must contain only alphanumeric characters and underscores");
        }
        if (c.getDucklakeDataPath() == null || c.getDucklakeDataPath().isBlank()) {
            throw new IllegalArgumentException("ducklakeDataPath is required for DuckLake connections");
        }
        if (c.getDucklakeMetadataDb() == null || c.getDucklakeMetadataDb().isBlank()) {
            throw new IllegalArgumentException("ducklakeMetadataDb is required for DuckLake connections");
        }
    }

    private java.sql.Connection openDuckDbConnection(String duckdbUrl, Connection original) throws SQLException {
        Connection temp = new Connection();
        temp.setUrl(duckdbUrl);
        temp.setDbType("duckdb");
        Driver driver = driverProvider.getDriver(temp);

        // Use auth if you support it; for local duckdb it’s usually empty
        Properties props = JDBCUtils.setJDBCAuth(temp);

        return driver.connect(duckdbUrl, props);
    }

    /** Build JDBC URL; for DuckLake use in-memory or session DB, not the metadata DB file. */
    private String buildDuckDbUrl(Connection c) {
        String url = c.getUrl();
        if (url != null && !url.isBlank()) {
            return url.startsWith("jdbc:duckdb:") ? url : "jdbc:duckdb:" + url;
        }
        String path = c.getDuckdbDatabasePath();
        if (path != null && !path.isBlank()) {
            return path.startsWith("jdbc:duckdb:") ? path : "jdbc:duckdb:" + path;
        }
        return "jdbc:duckdb:";
    }

    private Connection createDuckDbConnection(Connection original, String duckdbUrl, String catalogName) {
        Connection out = new Connection();
        out.setName(original.getName());
        out.setDatabaseName(catalogName);

        String schema = original.getSchemaName();
        if (schema == null || schema.isBlank()) schema = "main";

        out.setSchemaName(schema);
        out.setDbType("duckdb");
        out.setUrl(duckdbUrl);
        out.setUserName(original.getUserName());
        out.setPassword(original.getPassword());
        out.setTables(original.getTables());
        return out;
    }

    private String setupDuckLake(java.sql.Connection jdbc, Connection rosetta) throws SQLException {

        String catalogName = rosetta.getDatabaseName();
        String dataPath    = rosetta.getDucklakeDataPath();   // could be s3:// or local
        String metadataDb  = rosetta.getDucklakeMetadataDb();
        String schema      = rosetta.getSchemaName();
        if (schema == null || schema.isBlank()) schema = "main";

        try (Statement stmt = jdbc.createStatement()) {

            try { stmt.execute("INSTALL ducklake"); } catch (SQLException ignored) {}
            stmt.execute("LOAD ducklake");

            String attachSql;

            if (dataPath != null && dataPath.startsWith("s3://")) {
                try { stmt.execute("INSTALL httpfs"); } catch (SQLException ignored) {}
                stmt.execute("LOAD httpfs");
                applyS3Credentials(stmt, rosetta);
            }

            String safeCatalog = quoteIdentifier(catalogName);
            String safeSchema = quoteIdentifier(schema);
            attachSql = String.format("ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s');",
                    escapeSqlSingleQuotes(metadataDb), safeCatalog, escapeSqlSingleQuotes(dataPath));

            try {
                stmt.execute(attachSql);
            } catch (SQLException e) {
                String msg = e.getMessage() == null ? "" : e.getMessage();
                if (!msg.contains("already exists") && !msg.contains("Catalog with name")) {
                    throw e;
                }
            }

            stmt.execute("USE " + safeCatalog + "." + safeSchema + ";");
        }

        return catalogName;
    }

    private void applyS3Credentials(Statement stmt, Connection rosetta) throws SQLException {
        String region = rosetta.getS3Region();
        String accessKey = rosetta.getS3AccessKeyId();
        String secretKey = rosetta.getS3SecretAccessKey();
        if (region != null && !region.isBlank()) {
            stmt.execute("SET s3_region='" + region.replace("'", "''") + "';");
        }
        if (accessKey != null && !accessKey.isBlank()) {
            stmt.execute("SET s3_access_key_id='" + accessKey.replace("'", "''") + "';");
        }
        if (secretKey != null && !secretKey.isBlank()) {
            stmt.execute("SET s3_secret_access_key='" + secretKey.replace("'", "''") + "';");
        }
    }

    private static String escapeSqlSingleQuotes(String value) {
        return value == null ? "" : value.replace("'", "''");
    }

    /** Quote identifier for DuckDB (double-quote and escape any " inside). */
    private static String quoteIdentifier(String identifier) {
        if (identifier == null || identifier.isBlank()) return "\"main\"";
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    /**
     * List user tables from DuckLake metadata. User tables are not in information_schema under the
     * attached catalog; they are in __ducklake_metadata_&lt;catalog&gt;.ducklake_table.
     */
    private Collection<Table> listTablesFromDuckLakeMetadata(java.sql.Connection jdbc, String catalog, String schema) throws SQLException {
        String metadataCatalog = "\"__ducklake_metadata_" + catalog.replace("\"", "\"\"") + "\"";
        String sql = "SELECT DISTINCT t.table_name, s.schema_name FROM " + metadataCatalog + ".main.ducklake_table t " +
                "JOIN " + metadataCatalog + ".main.ducklake_schema s ON t.schema_id = s.schema_id " +
                "ORDER BY t.table_name, s.schema_name";
        List<Table> out = new ArrayList<>();
        try (PreparedStatement ps = jdbc.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String tableName = rs.getString(1);
                String schemaName = rs.getString(2);
                if (schemaName == null) schemaName = schema;
                if (!schema.equals(schemaName)) continue;
                Table t = new Table();
                t.setName(tableName);
                t.setSchema(schemaName);
                out.add(t);
            }
        }
        if (out.isEmpty()) {
            log.warn("DuckLake metadata has no user tables; ensure ducklakeMetadataDb is the same file your DataLake uses and that the catalog has been persisted.");
        }
        return out;
    }

    private Collection<Table> listTablesFallback(java.sql.Connection jdbc, String catalog, String schema) throws SQLException {
        String sql = "SELECT table_name FROM information_schema.tables " +
                "WHERE table_catalog = ? AND table_schema = ? AND table_type='BASE TABLE' " +
                "ORDER BY table_name";
        List<Table> out = new ArrayList<>();
        try (PreparedStatement ps = jdbc.prepareStatement(sql)) {
            ps.setString(1, catalog);
            ps.setString(2, schema);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Table t = new Table();
                    t.setName(rs.getString("table_name"));
                    t.setSchema(schema);
                    t.setType("BASE TABLE");
                    out.add(t);
                }
            }
        }
        return out;
    }

    private Collection<Table> filterDuckLakeMetadataTables(Collection<Table> allTables) {
        Set<String> metadataTableNames = Set.of(
                "ducklake_column", "ducklake_column_tag", "ducklake_data_file", "ducklake_delete_file",
                "ducklake_file_column_statistics", "ducklake_file_partition_value",
                "ducklake_files_scheduled_for_deletion", "ducklake_inlined_data_tables",
                "ducklake_metadata", "ducklake_partition_column", "ducklake_partition_info",
                "ducklake_schema", "ducklake_snapshot", "ducklake_snapshot_changes",
                "ducklake_table", "ducklake_table_column_stats", "ducklake_table_stats",
                "ducklake_tag", "ducklake_view", "ducklake_schema_settings", "ducklake_table_settings"
        );

        Collection<Table> userTables = new ArrayList<>();
        for (Table table : allTables) {
            if (table != null && table.getName() != null && !metadataTableNames.contains(table.getName())) {
                userTables.add(table);
            }
        }
        return userTables;
    }

    private TableExtractor loadDuckDbTableExtractor(Connection connection) {
        Optional<Class<?>> mod = ModuleLoader.loadModuleByAnnotationClassValues(
                DefaultTablesExtractor.class.getPackageName(), RosettaModuleTypes.TABLE_EXTRACTOR, connection.getDbType());
        if (mod.isEmpty()) {
            log.warn("DuckDB table extractor not found, falling back to default.");
            return new DefaultTablesExtractor();
        }
        try {
            return (TableExtractor) mod.get().getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException("Failed to instantiate DuckDB table extractor", e);
        }
    }

    private ViewExtractor loadDuckDbViewExtractor(Connection connection) {
        Optional<Class<?>> mod = ModuleLoader.loadModuleByAnnotationClassValues(
                DefaultViewExtractor.class.getPackageName(), RosettaModuleTypes.VIEW_EXTRACTOR, connection.getDbType());
        if (mod.isEmpty()) {
            log.warn("DuckDB view extractor not found, falling back to default.");
            return new DefaultViewExtractor();
        }
        try {
            return (ViewExtractor) mod.get().getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException("Failed to instantiate DuckDB view extractor", e);
        }
    }

    private ColumnExtractor loadDuckDbColumnExtractor(Connection connection) {
        Optional<Class<?>> mod = ModuleLoader.loadModuleByAnnotationClassValues(
                ColumnsExtractor.class.getPackageName(), RosettaModuleTypes.COLUMN_EXTRACTOR, connection.getDbType());
        if (mod.isEmpty()) {
            log.warn("DuckDB column extractor not found, falling back to default.");
            return new ColumnsExtractor(connection);
        }
        try {
            return (ColumnExtractor) mod.get().getDeclaredConstructor(Connection.class).newInstance(connection);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException("Failed to instantiate DuckDB column extractor", e);
        }
    }
}