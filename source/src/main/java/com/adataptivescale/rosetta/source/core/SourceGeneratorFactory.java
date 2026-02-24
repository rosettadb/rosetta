package com.adataptivescale.rosetta.source.core;

import com.adaptivescale.rosetta.common.JDBCDriverProvider;
import com.adaptivescale.rosetta.common.helpers.ModuleLoader;
import com.adaptivescale.rosetta.common.models.Database;
import com.adaptivescale.rosetta.common.DriverManagerDriverProvider;
import com.adaptivescale.rosetta.common.models.input.Connection;
import com.adaptivescale.rosetta.common.types.RosettaModuleTypes;
import com.adataptivescale.rosetta.source.core.extractors.column.*;
import com.adataptivescale.rosetta.source.core.extractors.table.DefaultTablesExtractor;
import com.adataptivescale.rosetta.source.core.extractors.view.DefaultViewExtractor;
import com.adataptivescale.rosetta.source.core.interfaces.Generator;
import com.adataptivescale.rosetta.source.core.interfaces.TableExtractor;
import com.adataptivescale.rosetta.source.core.interfaces.ViewExtractor;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

@Slf4j
public class SourceGeneratorFactory {
    public static Generator<Database, Connection> sourceGenerator(Connection connection) {
        return sourceGenerator(connection, new DriverManagerDriverProvider());
    }

    private static ColumnsExtractor loadColumnExtractor(Connection connection) {
        Optional<Class<?>> columnExtractorModule = ModuleLoader.loadModuleByAnnotationClassValues(
                ColumnsExtractor.class.getPackageName(), RosettaModuleTypes.COLUMN_EXTRACTOR, connection.getDbType());
        if(columnExtractorModule.isEmpty()) {
            log.warn("Columns extractor not supported for database type: {} falling back to default. ", connection.getDbType());
            return new ColumnsExtractor(connection);
        }
        try {
            return (ColumnsExtractor) columnExtractorModule.get().getDeclaredConstructor(
                    Connection.class).newInstance(connection);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static TableExtractor loadTableExtractor(Connection connection) {
        Optional<Class<?>> tableExtractorModule = ModuleLoader.loadModuleByAnnotationClassValues(
                DefaultTablesExtractor.class.getPackageName(), RosettaModuleTypes.TABLE_EXTRACTOR, connection.getDbType());
        if(tableExtractorModule.isEmpty()) {
            log.warn("Table extractor not supported for database type: {} falling back to default.", connection.getDbType());
            return new DefaultTablesExtractor();
        }
        try {
            return (TableExtractor) tableExtractorModule.get().getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static ViewExtractor loadViewExtractor(Connection connection) {
        Optional<Class<?>> viewExtractorModule = ModuleLoader.loadModuleByAnnotationClassValues(
                DefaultViewExtractor.class.getPackageName(), RosettaModuleTypes.VIEW_EXTRACTOR, connection.getDbType());
        if(viewExtractorModule.isEmpty()) {
            log.warn("View extractor not supported for database type: {} falling back to default.", connection.getDbType());
            return new DefaultViewExtractor();
        }
        try {
            return (ViewExtractor) viewExtractorModule.get().getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public static Generator<Database, Connection> sourceGenerator(Connection connection, JDBCDriverProvider driverProvider) {
        // Check if this is a DuckLake connection
        if ("ducklake".equalsIgnoreCase(connection.getDbType())) {
            log.debug("Detected DuckLake connection, using DuckLakeGenerator");
            return new DuckLakeGenerator(driverProvider);
        }
        
        TableExtractor tablesExtractor = loadTableExtractor(connection);
        ViewExtractor viewExtractor = loadViewExtractor(connection);
        ColumnsExtractor columnsExtractor = loadColumnExtractor(connection);
        return new DefaultGenerator(tablesExtractor, viewExtractor, columnsExtractor, driverProvider);
    }
}
