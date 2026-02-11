package com.adataptivescale.rosetta.source.core;

import com.adaptivescale.rosetta.common.JDBCDriverProvider;
import com.adaptivescale.rosetta.common.JDBCUtils;
import com.adaptivescale.rosetta.common.DriverManagerDriverProvider;
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

            // Try normal extractor
            Collection<Table> allTables;
            try {
                allTables = (Collection<Table>) tableExtractor.extract(duckdbConnection, jdbc);
            } catch (Exception e) {
                log.warn("Table extractor failed, falling back to information_schema query: {}", e.getMessage());
                allTables = listTablesFallback(jdbc, catalog, duckdbConnection.getSchemaName());
            }

            Collection<Table> tables = filterDuckLakeMetadataTables(allTables);
            log.info("Extracted {} user tables from {}.{}", tables.size(), catalog, duckdbConnection.getSchemaName());

            // columns
            colExtractor.extract(jdbc, tables);

            // views
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

    private void validateDuckLakeConfig(Connection c) {
        if (c.getDatabaseName() == null || c.getDatabaseName().isBlank()) {
            throw new IllegalArgumentException("databaseName is required for DuckLake connections");
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

        log.info("Opening DuckDB JDBC session: {}", duckdbUrl);
        return driver.connect(duckdbUrl, props);
    }

    /**
     * URL rules:
     * - If connection.url is set and already starts with jdbc:duckdb:, use it as-is.
     * - Else if duckdbDatabasePath is set and looks like a path, prefix with jdbc:duckdb:
     * - Else default to jdbc:duckdb: (in-memory)
     *
     * IMPORTANT: for DuckLake, DO NOT use the metadata DB file as the main db.
     */
    private String buildDuckDbUrl(Connection c) {
        String url = c.getUrl();
        if (url != null && !url.isBlank()) {
            if (url.startsWith("jdbc:duckdb:")) {
                return url;
            }
            // if user accidentally provided plain path in url, still support it
            return "jdbc:duckdb:" + url;
        }

        String path = c.getDuckdbDatabasePath();
        if (path != null && !path.isBlank()) {
            // If someone mistakenly put "jdbc:duckdb:" into duckdbDatabasePath, just return it cleanly.
            if (path.startsWith("jdbc:duckdb:")) {
                return path;
            }
            return "jdbc:duckdb:" + path;
        }

        return "jdbc:duckdb:"; // in-memory
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
        String dataPath    = rosetta.getDucklakeDataPath();
        String metadataDb  = rosetta.getDucklakeMetadataDb();
        String schema = rosetta.getSchemaName();
        if (schema == null || schema.isBlank()) schema = "main";

        try (Statement stmt = jdbc.createStatement()) {
            try { stmt.execute("INSTALL ducklake"); } catch (SQLException ignored) {}
            stmt.execute("LOAD ducklake");

            // Helpful debug
            logDatabaseList(jdbc);

            String attachSql = String.format(
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s', METADATA_PATH '%s');",
                    dataPath, catalogName, dataPath, metadataDb
            );

            log.info("Attaching DuckLake catalog: {}", attachSql);
            try {
                stmt.execute(attachSql);
            } catch (SQLException e) {
                String msg = e.getMessage() == null ? "" : e.getMessage();
                // allow reruns
                if (msg.contains("already exists") || msg.contains("Catalog with name")) {
                    log.info("Catalog '{}' already attached, continuing.", catalogName);
                } else {
                    throw e;
                }
            }

            // Always use catalog.main (matches your terminal usage)
            stmt.execute("USE " + catalogName + "." + schema + ";");
        }

        // sanity: verify we can list tables
        logDuckLakeTables(jdbc, catalogName, schema);

        return catalogName;
    }

    private void logDatabaseList(java.sql.Connection jdbc) {
        try (Statement s = jdbc.createStatement();
             ResultSet rs = s.executeQuery("PRAGMA database_list;")) {
            while (rs.next()) {
                String name = rs.getString("name");
                String file = rs.getString("file");
                log.info("database_list: name={} file={}", name, file);
            }
        } catch (SQLException ignored) {}
    }

    private void logDuckLakeTables(java.sql.Connection jdbc, String catalog, String schema) {
        String sql = "SELECT table_name FROM information_schema.tables " +
                "WHERE table_catalog = ? AND table_schema = ? ORDER BY table_name";
        try (PreparedStatement ps = jdbc.prepareStatement(sql)) {
            ps.setString(1, catalog);
            ps.setString(2, schema);
            try (ResultSet rs = ps.executeQuery()) {
                int n = 0;
                while (rs.next()) {
                    n++;
                    log.info("ducklake table: {}.{}.{}", catalog, schema, rs.getString(1));
                }
                log.info("ducklake tables total ({}.{}) = {}", catalog, schema, n);
            }
        } catch (SQLException e) {
            log.warn("Could not enumerate tables via information_schema: {}", e.getMessage());
        }
    }

    // fallback table listing (if your extractor uses DatabaseMetaData and misses attached catalogs)
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
                    out.add(t);
                }
            }
        }
        return out;
    }

    // Filters out DuckLake internal metadata tables
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

    // Helper method to execute SQL commands (fixed URL building)
    public static void executeDebugSQL(Connection connection, String sql) throws Exception {
        DuckLakeGenerator generator = new DuckLakeGenerator(new DriverManagerDriverProvider());
        String duckdbUrl = generator.buildDuckDbUrl(connection);

        java.sql.Connection jdbc = generator.openDuckDbConnection(duckdbUrl, connection);
        try {
            generator.setupDuckLake(jdbc, connection);

            try (Statement stmt = jdbc.createStatement()) {
                log.info("Executing SQL: {}", sql);
                boolean hasResults = stmt.execute(sql);
                if (hasResults) {
                    try (ResultSet rs = stmt.getResultSet()) {
                        int colCount = rs.getMetaData().getColumnCount();
                        while (rs.next()) {
                            StringBuilder row = new StringBuilder("  ");
                            for (int i = 1; i <= colCount; i++) {
                                if (i > 1) row.append(" | ");
                                row.append(rs.getString(i));
                            }
                            log.info(row.toString());
                        }
                    }
                } else {
                    log.info("SQL executed. Rows affected: {}", stmt.getUpdateCount());
                }
            }
        } finally {
            try { jdbc.close(); } catch (SQLException ignored) {}
        }
    }
}