package com.adataptivescale.rosetta.source.dbt;

import com.adaptivescale.rosetta.common.models.Database;
import com.adaptivescale.rosetta.common.models.Table;
import com.adaptivescale.rosetta.common.models.View;
import com.adaptivescale.rosetta.common.models.dbt.DbtColumn;
import com.adaptivescale.rosetta.common.models.dbt.DbtModel;
import com.adaptivescale.rosetta.common.models.dbt.DbtSource;
import com.adaptivescale.rosetta.common.models.dbt.DbtTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DbtModelGenerator {

    public static DbtModel dbtModelGenerator(List<Database> databases, boolean includeViews) {
        DbtModel dbtModel = new DbtModel();
        dbtModel.setVersion(2);
        Collection<DbtSource> dbtSources = new ArrayList<>();
        databases.forEach(database -> {
            prepareDbtSource(dbtSources, database, includeViews);
        });
        dbtModel.setSources(dbtSources);
        return dbtModel;
    }

    public static DbtModel dbtModelGenerator(List<Database> databases) {
        return dbtModelGenerator(databases, false);
    }

    private static void prepareDbtSource(Collection<DbtSource> dbtSources, Database database, boolean includeViews) {
        Map<String, List<Table>> tablesBySchema = database.getTables().stream().collect(Collectors.groupingBy(Table::getSchema));
        Map<String, List<View>> viewsBySchema = database.getViews().stream().collect(Collectors.groupingBy(View::getSchema));

        Collection<String> allSchemas = new ArrayList<>(tablesBySchema.keySet());
        if (includeViews) {
            allSchemas.addAll(viewsBySchema.keySet());
        }

        allSchemas.stream().distinct().forEach(schemaName -> {
            DbtSource dbtSource = new DbtSource();
            dbtSource.setName(schemaName);
            dbtSource.setDescription(database.getDatabaseType());
            Collection<DbtTable> dbtTables = new ArrayList<>();


            List<Table> tablesInSchema = tablesBySchema.get(schemaName);
            if (tablesInSchema != null) {
                tablesInSchema.forEach(table -> {
                    DbtTable dbtTable = createDbtTableFromTable(table);
                    dbtTables.add(dbtTable);
                });
            }

            if (includeViews) {
                List<View> viewsInSchema = viewsBySchema.get(schemaName);
                if (viewsInSchema != null) {
                    viewsInSchema.forEach(view -> {
                        DbtTable dbtTable = createDbtTableFromView(view);
                        dbtTables.add(dbtTable);
                    });
                }
            }

            if (!dbtTables.isEmpty()) {
                dbtSource.setTables(dbtTables);
                dbtSources.add(dbtSource);
            }
        });
    }

    private static DbtTable createDbtTableFromTable(Table table) {
        DbtTable dbtTable = new DbtTable();
        dbtTable.setName(table.getName());
        Collection<DbtColumn> dbtColumns = new ArrayList<>();

        if (table.getColumns() == null) {
            return dbtTable;
        }

        table.getColumns().forEach(column -> {
            Collection<String> tests = new ArrayList<>();
            DbtColumn dbtColumn = new DbtColumn();
            dbtColumn.setName(column.getName());
            dbtColumn.setDescription(column.getDescription());
            if (!column.isNullable()) {
                tests.add("not_null");
            }
            if (column.isPrimaryKey()) {
                tests.add("unique");
            }
            dbtColumn.setTests(tests);
            dbtColumns.add(dbtColumn);
        });

        dbtTable.setColumns(dbtColumns);
        return dbtTable;
    }

    private static DbtTable createDbtTableFromView(View view) {
        DbtTable dbtTable = new DbtTable();
        dbtTable.setName(view.getName());
        Collection<DbtColumn> dbtColumns = new ArrayList<>();

        view.getColumns().forEach(column -> {
            Collection<String> tests = new ArrayList<>();
            DbtColumn dbtColumn = new DbtColumn();
            dbtColumn.setName(column.getName());
            dbtColumn.setDescription(column.getDescription());
            if (!column.isNullable()) {
                tests.add("not_null");
            }
            if (column.isPrimaryKey()) {
                tests.add("unique");
            }
            dbtColumn.setTests(tests);
            dbtColumns.add(dbtColumn);
        });

        dbtTable.setColumns(dbtColumns);
        return dbtTable;
    }

    public static Map<String, String> dbtSQLGenerator(DbtModel dbtModel, Boolean isIncremental) {
        Map<String, String> tables = new HashMap<>();

        if (dbtModel.getSources() != null && !dbtModel.getSources().isEmpty()) {
            dbtModel.getSources().forEach(dbtSource -> {
                dbtSource.getTables().forEach(dbtTable -> {
                    StringBuilder table = new StringBuilder();
                    table.append("with ").append(dbtTable.getName()).append(" as (");
                    table.append("\n\t");
                    table.append("select\n\t\t");
                    table.append(dbtTable.getColumns().stream().map(DbtColumn::getName).collect(Collectors.joining(",\n\t\t")));
                    table.append("\n\t");
                    table.append(String.format("from {{ source('%s', '%s') }}", dbtSource.getName(), dbtTable.getName()));
                    table.append("\n)\n\n");
                    table.append("select * from ").append(dbtTable.getName());

                    tables.put(String.format("%s_%s", dbtSource.getName(), dbtTable.getName()), table.toString());
                });
            });
        } else if (dbtModel.getModels() != null && !dbtModel.getModels().isEmpty()) {
            dbtModel.getModels().forEach(dbtTable -> {
                StringBuilder table = new StringBuilder();
                table.append("with ").append(dbtTable.getName()).append(" as (");
                table.append("\n\t");
                table.append("select\n\t\t");
                table.append(dbtTable.getColumns().stream().map(DbtColumn::getName).collect(Collectors.joining(",\n\t\t")));
                table.append("\n\t");
                table.append(String.format("from {{ ref('%s') }}", dbtTable.getName()));
                table.append("\n)\n\n");
                table.append("select * from ").append(dbtTable.getName());

                tables.put(dbtTable.getName(), table.toString());
            });
        }

        return tables;
    }

    public static Map<String, String> dbtSQLGenerator(List<DbtModel> dbtModels, Boolean isIncremental) {
        Map<String, String> tables = new HashMap<>();

        dbtModels.forEach(dbtModel -> {
            tables.putAll(dbtSQLGenerator(dbtModel, isIncremental));
        });

        return tables;
    }
}