package com.adataptivescale.rosetta.source.core;

import com.adaptivescale.rosetta.common.JDBCDriverProvider;
import com.adaptivescale.rosetta.common.JDBCUtils;
import com.adaptivescale.rosetta.common.DriverManagerDriverProvider;
import com.adaptivescale.rosetta.common.models.Database;
import com.adaptivescale.rosetta.common.models.Table;
import com.adaptivescale.rosetta.common.models.View;
import com.adaptivescale.rosetta.common.models.input.Connection;
import com.adaptivescale.rosetta.common.helpers.ModuleLoader;
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
        if (connection.getDucklakeDataPath() == null || connection.getDucklakeDataPath().trim().isEmpty()) {
            throw new IllegalArgumentException("ducklakeDataPath is required for DuckLake connections");
        }

        String duckdbUrl = buildDuckDbUrl(connection);
        Connection tempConnection = new Connection();
        tempConnection.setUrl(duckdbUrl);
        tempConnection.setDbType("duckdb");
        Driver driver = driverProvider.getDriver(tempConnection);
        Properties properties = JDBCUtils.setJDBCAuth(tempConnection);
        java.sql.Connection connect = driver.connect(duckdbUrl, properties);

        try {
            String attachedCatalogAlias = setupDuckLake(connect, connection);
            String actualCatalogWithTables = findCatalogWithTables(connect);
            if (actualCatalogWithTables != null) {
                log.info("Using catalog '{}' for extraction", actualCatalogWithTables);
                attachedCatalogAlias = actualCatalogWithTables;
            }

            Connection duckdbConnection = createDuckDbConnection(connection, duckdbUrl, attachedCatalogAlias);
            TableExtractor tableExtractor = loadDuckDbTableExtractor(duckdbConnection);
            ViewExtractor viewExtractor = loadDuckDbViewExtractor(duckdbConnection);
            ColumnExtractor columnsExtractor = loadDuckDbColumnExtractor(duckdbConnection);

            Collection<Table> allTables = (Collection<Table>) tableExtractor.extract(duckdbConnection, connect);
            Collection<Table> tables = filterDuckLakeMetadataTables(allTables);
            log.info("Extracted {} user tables from catalog '{}'", tables.size(), attachedCatalogAlias);

            columnsExtractor.extract(connect, tables);
            Collection<View> views = (Collection<View>) viewExtractor.extract(duckdbConnection, connect);
            log.info("Extracted {} views", views.size());
            columnsExtractor.extract(connect, views);

            Database database = new Database();
            database.setName(connect.getMetaData().getDatabaseProductName());
            database.setTables(tables);
            database.setViews(views);
            database.setDatabaseType(connection.getDbType());
            return database;
        } finally {
            connect.close();
        }
    }

    @Override
    public Database validate(Connection connection) throws Exception {
        if (connection.getDucklakeDataPath() == null || connection.getDucklakeDataPath().trim().isEmpty()) {
            throw new IllegalArgumentException("ducklakeDataPath is required for DuckLake connections");
        }

        String duckdbUrl = buildDuckDbUrl(connection);
        Connection tempConnection = new Connection();
        tempConnection.setUrl(duckdbUrl);
        tempConnection.setDbType("duckdb");
        Driver driver = driverProvider.getDriver(tempConnection);
        Properties properties = JDBCUtils.setJDBCAuth(tempConnection);
        java.sql.Connection connect = driver.connect(duckdbUrl, properties);

        try {
            setupDuckLake(connect, connection);
            Database database = new Database();
            database.setName(connect.getMetaData().getDatabaseProductName());
            return database;
        } finally {
            connect.close();
        }
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
            "ducklake_tag", "ducklake_view"
        );

        Collection<Table> userTables = new ArrayList<>();
        for (Table table : allTables) {
            if (!metadataTableNames.contains(table.getName())) {
                userTables.add(table);
            }
        }
        return userTables;
    }

    // Finds catalog with user tables
    private String findCatalogWithTables(java.sql.Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT DISTINCT table_catalog FROM information_schema.tables " +
                 "WHERE table_catalog NOT LIKE '__ducklake_metadata%' " +
                 "AND table_catalog NOT IN ('system', 'temp') LIMIT 1")) {
            if (rs.next()) {
                return rs.getString("table_catalog");
            }
        }
        return null;
    }

    private String buildDuckDbUrl(Connection connection) {
        if (connection.getDuckdbDatabasePath() != null && !connection.getDuckdbDatabasePath().trim().isEmpty()) {
            return "jdbc:duckdb:" + connection.getDuckdbDatabasePath();
        }
        return "jdbc:duckdb:";
    }

    private Connection createDuckDbConnection(Connection originalConnection, String duckdbUrl, String catalogName) {
        Connection duckdbConnection = new Connection();
        duckdbConnection.setName(originalConnection.getName());
        duckdbConnection.setDatabaseName(catalogName);
        String schemaName = originalConnection.getSchemaName();
        if (schemaName == null || schemaName.trim().isEmpty()) {
            schemaName = "main";
        }
        duckdbConnection.setSchemaName(schemaName);
        duckdbConnection.setDbType("duckdb");
        duckdbConnection.setUrl(duckdbUrl);
        duckdbConnection.setUserName(originalConnection.getUserName());
        duckdbConnection.setPassword(originalConnection.getPassword());
        duckdbConnection.setTables(originalConnection.getTables());
        return duckdbConnection;
    }

    private String setupDuckLake(java.sql.Connection connection, Connection rosettaConnection) throws SQLException {
        Statement stmt = connection.createStatement();
        try {
            try {
                stmt.execute("INSTALL ducklake;");
            } catch (SQLException e) {
                // Already installed
            }
            stmt.execute("LOAD ducklake;");

            String catalogName = rosettaConnection.getDatabaseName();
            if (catalogName == null || catalogName.trim().isEmpty()) {
                throw new IllegalArgumentException("databaseName is required for DuckLake connections");
            }

            String metadataDb = rosettaConnection.getDucklakeMetadataDb();
            if (metadataDb != null && !metadataDb.trim().isEmpty()) {
                java.io.File metadataFile = new java.io.File(metadataDb);
                String fileName = metadataFile.getName();
                if (fileName.endsWith(".duckdb")) {
                    fileName = fileName.substring(0, fileName.length() - 7);
                }
                metadataDb = fileName + ".ducklake";
            } else {
                metadataDb = catalogName.toLowerCase() + ".ducklake";
            }

            String attachSql = String.format(
                "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s');",
                metadataDb, catalogName, rosettaConnection.getDucklakeDataPath()
            );

            try {
                log.info("Attaching DuckLake catalog: {}", attachSql);
                stmt.execute(attachSql);
            } catch (SQLException e) {
                if (e.getMessage() != null && e.getMessage().contains("already exists")) {
                    log.info("Catalog '{}' is already attached", catalogName);
                } else {
                    throw e;
                }
            }

            try (Statement useStmt = connection.createStatement()) {
                useStmt.execute("USE " + catalogName + ";");
            }

            registerParquetFiles(connection, rosettaConnection, catalogName);
            return catalogName;
        } finally {
            stmt.close();
        }
    }

    // Registers parquet files from data path as tables
    private void registerParquetFiles(java.sql.Connection connection, Connection rosettaConnection, String catalogName) throws SQLException {
        java.io.File dataDir = new java.io.File(rosettaConnection.getDucklakeDataPath());
        if (!dataDir.exists() || !dataDir.isDirectory()) {
            return;
        }

        java.io.File[] parquetFiles = dataDir.listFiles((dir, name) ->
            name.toLowerCase().endsWith(".parquet") || name.toLowerCase().endsWith(".parq"));

        if (parquetFiles == null || parquetFiles.length == 0) {
            return;
        }

        log.info("Registering {} parquet file(s) as tables", parquetFiles.length);
        try (Statement createStmt = connection.createStatement()) {
            for (java.io.File parquetFile : parquetFiles) {
                String fileName = parquetFile.getName();
                String tableName = fileName;
                if (tableName.endsWith(".parquet")) {
                    tableName = tableName.substring(0, tableName.length() - 8);
                } else if (tableName.endsWith(".parq")) {
                    tableName = tableName.substring(0, tableName.length() - 5);
                }
                tableName = tableName.replaceAll("[^a-zA-Z0-9_]", "_");

                try {
                    String createTableSql = String.format(
                        "CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM read_parquet('%s');",
                        tableName, parquetFile.getAbsolutePath()
                    );
                    createStmt.execute(createTableSql);
                    log.info("Registered parquet file '{}' as table '{}'", fileName, tableName);
                } catch (SQLException e) {
                    log.warn("Could not register parquet file '{}': {}", fileName, e.getMessage());
                }
            }
        }
    }

    private TableExtractor loadDuckDbTableExtractor(Connection connection) {
        Optional<Class<?>> tableExtractorModule = ModuleLoader.loadModuleByAnnotationClassValues(
                DefaultTablesExtractor.class.getPackageName(), RosettaModuleTypes.TABLE_EXTRACTOR, connection.getDbType());
        if (tableExtractorModule.isEmpty()) {
            log.warn("DuckDB table extractor not found, falling back to default.");
            return new DefaultTablesExtractor();
        }
        try {
            return (TableExtractor) tableExtractorModule.get().getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException("Failed to instantiate DuckDB table extractor", e);
        }
    }

    private ViewExtractor loadDuckDbViewExtractor(Connection connection) {
        Optional<Class<?>> viewExtractorModule = ModuleLoader.loadModuleByAnnotationClassValues(
                DefaultViewExtractor.class.getPackageName(), RosettaModuleTypes.VIEW_EXTRACTOR, connection.getDbType());
        if (viewExtractorModule.isEmpty()) {
            log.warn("DuckDB view extractor not found, falling back to default.");
            return new DefaultViewExtractor();
        }
        try {
            return (ViewExtractor) viewExtractorModule.get().getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException("Failed to instantiate DuckDB view extractor", e);
        }
    }

    private ColumnExtractor loadDuckDbColumnExtractor(Connection connection) {
        Optional<Class<?>> columnExtractorModule = ModuleLoader.loadModuleByAnnotationClassValues(
                ColumnsExtractor.class.getPackageName(), RosettaModuleTypes.COLUMN_EXTRACTOR, connection.getDbType());
        if (columnExtractorModule.isEmpty()) {
            log.warn("DuckDB column extractor not found, falling back to default.");
            return new ColumnsExtractor(connection);
        }
        try {
            return (ColumnExtractor) columnExtractorModule.get().getDeclaredConstructor(
                    Connection.class).newInstance(connection);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException("Failed to instantiate DuckDB column extractor", e);
        }
    }

    // Helper method to execute SQL commands
    public static void executeDebugSQL(Connection connection, String sql) throws Exception {
        if (connection.getDucklakeDataPath() == null || connection.getDucklakeDataPath().trim().isEmpty()) {
            throw new IllegalArgumentException("ducklakeDataPath is required for DuckLake connections");
        }

        String duckdbUrl = connection.getDuckdbDatabasePath() != null && !connection.getDuckdbDatabasePath().trim().isEmpty()
            ? "jdbc:duckdb:" + connection.getDuckdbDatabasePath()
            : "jdbc:duckdb:";

        Connection tempConnection = new Connection();
        tempConnection.setUrl(duckdbUrl);
        tempConnection.setDbType("duckdb");
        Driver driver = new DriverManagerDriverProvider().getDriver(tempConnection);
        Properties properties = JDBCUtils.setJDBCAuth(tempConnection);
        java.sql.Connection connect = driver.connect(duckdbUrl, properties);

        try {
            DuckLakeGenerator generator = new DuckLakeGenerator(new DriverManagerDriverProvider());
            generator.setupDuckLake(connect, connection);

            try (Statement stmt = connect.createStatement()) {
                log.info("Executing SQL: {}", sql);
                boolean hasResults = stmt.execute(sql);
                if (hasResults) {
                    try (ResultSet rs = stmt.getResultSet()) {
                        log.info("Query returned results:");
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
                    log.info("SQL executed successfully. Rows affected: {}", stmt.getUpdateCount());
                }
            }
        } finally {
            connect.close();
        }
    }

    // Helper method to import CSV file into DuckLake catalog
    public static void importCsvToDuckLake(Connection connection, String csvFilePath, String tableName) throws Exception {
        String catalogName = connection.getDatabaseName();
        if (catalogName == null || catalogName.trim().isEmpty()) {
            throw new IllegalArgumentException("databaseName must be set to the DuckLake catalog name");
        }

        String sql = String.format(
            "USE %s; CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s');",
            catalogName, tableName, csvFilePath
        );

        log.info("Importing CSV file '{}' as table '{}' in catalog '{}'", csvFilePath, tableName, catalogName);
        executeDebugSQL(connection, sql);
        log.info("Successfully imported CSV file!");
    }
}
