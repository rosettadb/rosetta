package com.adaptivescale.rosetta.ddl.targets.spanner;

import com.adaptivescale.rosetta.common.annotations.RosettaModule;
import com.adaptivescale.rosetta.common.models.*;
import com.adaptivescale.rosetta.common.types.RosettaModuleTypes;
import com.adaptivescale.rosetta.ddl.DDL;
import com.adaptivescale.rosetta.ddl.change.model.ColumnChange;
import com.adaptivescale.rosetta.ddl.change.model.ForeignKeyChange;
import com.adaptivescale.rosetta.ddl.targets.ColumnSQLDecoratorFactory;
import com.adaptivescale.rosetta.ddl.utils.TemplateEngine;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

import static com.adaptivescale.rosetta.ddl.targets.spanner.Constants.DEFAULT_WRAPPER;
import static java.util.Comparator.*;

@Slf4j
@RosettaModule(
        name = "spanner",
        type = RosettaModuleTypes.DDL_GENERATOR
)
public class SpannerDDLGenerator implements DDL {
    private final static String TABLE_CREATE_TEMPLATE = "spanner/table/create";
    private final static String TABLE_DROP_TEMPLATE = "spanner/table/drop";
    private final static String FOREIGN_KEY_CREATE_TEMPLATE = "spanner/foreignkey/create";
    private final static String FOREIGN_KEY_DROP_TEMPLATE = "spanner/foreignkey/drop";
    private final static String COLUMN_ADD_TEMPLATE = "spanner/column/add";
    private final static String COLUMN_DROP_TEMPLATE = "spanner/column/drop";
    private static String VIEW_CREATE_TEMPLATE = "spanner/view/create";
    private static String VIEW_ALTER_TEMPLATE = "spanner/view/alter";
    private static String VIEW_DROP_TEMPLATE = "spanner/view/drop";

    private final ColumnSQLDecoratorFactory columnSQLDecoratorFactory = new SpannerColumnDecoratorFactory();

    @Override
    public String createColumn(Column column) {
        return columnSQLDecoratorFactory.decoratorFor(column).expressSQl();
    }

    @Override
    public String createTable(Table table, boolean dropTableIfExists) {
        if(table.getColumns().stream().filter(column -> column.isPrimaryKey()).count()==0){
            throw new RuntimeException(String.format("Table %s has no primary key. Spanner requires tables to have primary key.", table.getName()));
        }
        List<String> definitions = table.getColumns().stream().map(this::createColumn).collect(Collectors.toList());
        Map<String, Object> createParams = new HashMap<>();
        Optional<String> primaryKeysForTable = createPrimaryKeysForTable(table);
        String definitionAsString = String.join(", ", definitions);

        StringBuilder stringBuilder = new StringBuilder();
        if (dropTableIfExists) {
            stringBuilder.append(dropTable(table));
        }


        if (table.getInterleave() != null) {
            final String onDeleteAction = table.getInterleave().getOnDeleteAction();
            if (onDeleteAction != null && !onDeleteAction.isEmpty()) {
                createParams.put("deleteRule", String.format("ON DELETE %s", table.getInterleave().getOnDeleteAction()));
            }
            createParams.put("interleavedTable", table.getInterleave().getParentName());
        }
        createParams.put("schemaName", table.getSchema());
        createParams.put("tableName", table.getName());
        createParams.put("tableCode", definitionAsString);
        primaryKeysForTable.ifPresent(s -> createParams.put("primaryKeyDefinition", s));
        stringBuilder.append(TemplateEngine.process(TABLE_CREATE_TEMPLATE, createParams));
        return stringBuilder.toString();
    }

    @Override
    public String createTableSchema(Table table) {
        return "";
    }

    @Override
    public String createDatabase(Database database, boolean dropTableIfExists) {
        StringBuilder stringBuilder = new StringBuilder();

        Set<String> schemas = database.getTables().stream().map(Table::getSchema).filter(s -> s != null && !s.isEmpty()).collect(Collectors.toSet());
        if (!schemas.isEmpty()) {
            throw new RuntimeException("Schema is not supported in Spanner");
        }
        List<String> missingPrimaryKeys = new ArrayList<>();
        database.getTables().forEach(table -> {
            if(table.getColumns().stream().filter(column -> column.isPrimaryKey()).count()==0){
                missingPrimaryKeys.add(table.getName());
            }
        });
        if(!missingPrimaryKeys.isEmpty()){
            throw new RuntimeException(
                String.format("Tables %s are missing primary key. Spanner does not allow table without primary key.",
                    missingPrimaryKeys.stream().collect(Collectors.joining(","))));
        }
        List<Table> tablesToCreate = database.getTables().stream().collect(Collectors.toList());

        //Make sure you create interleaved tables at the end
        tablesToCreate.sort(nullsFirst(
            comparing(it -> Optional.ofNullable(it.getInterleave())
                .map(Interleave::getTableName)
                .orElse(null), nullsFirst(naturalOrder()))));

        stringBuilder.append(tablesToCreate
            .stream()
            .map(table -> createTable(table, dropTableIfExists))
            .collect(Collectors.joining("\r\r")));

        String foreignKeys = database
            .getTables()
            .stream()
            .map(this::foreignKeys)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.joining());

        if (!foreignKeys.isEmpty()) {
            stringBuilder.append("\r").append(foreignKeys).append("\r");
        }

        String indices = database
            .getTables()
            .stream()
            .map(this::createIndicesForTable)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.joining());

        if (!indices.isEmpty()) {
            stringBuilder.append("\r").append(indices).append("\r");
        }

        if (database.getViews() != null && !database.getViews().isEmpty()) {
            stringBuilder.append("\r").append(database.getViews()
                    .stream()
                    .map(view -> createView(view, dropTableIfExists))
                    .collect(Collectors.joining("\r\r")));
        }

        return stringBuilder.toString();
    }

    @Override
    public String createForeignKey(ForeignKey foreignKey) {
        //For foreign keys returned as result of interleave table, JDBC returns no name.
        // Those are created automatically by JDBC once we specify that the table is interleaved.
        if (foreignKey.getName() == null) {
            return "";
        }
        Map<String, Object> params = new HashMap<>();
        params.put("schemaName", foreignKey.getSchema());
        params.put("tableName", foreignKey.getTableName());
        params.put("foreignkeyColumn", foreignKey.getColumnName());
        params.put("primaryTableSchema", foreignKey.getPrimaryTableSchema());
        params.put("primaryTableName", foreignKey.getPrimaryTableName());
        params.put("foreignKeyPrimaryColumnName", foreignKey.getPrimaryColumnName());
        params.put("foreignkeyName", foreignKey.getName());
        params.put("deleteRule", foreignKeyDeleteRule(foreignKey));
        return TemplateEngine.process(FOREIGN_KEY_CREATE_TEMPLATE, params);
    }

    @Override
    public String alterColumn(ColumnChange change) {
        Table table = change.getTable();
        Column actual = change.getActual();
        Column expected = change.getExpected();

        if (!Objects.equals(expected.getTypeName(), actual.getTypeName())
                || !Objects.equals(expected.isNullable(), actual.isNullable())) {
            String alterColumnString = columnSQLDecoratorFactory.decoratorFor(expected).expressSQl();
            String formattedAlterColumn = String.format("%s %s", alterColumnString.split(" ")[0], alterColumnString.split(" ")[1]);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("ALTER TABLE");
            stringBuilder.append(handleNullSchema(table.getSchema(), table.getName()));
            stringBuilder.append(" ALTER COLUMN ");
            stringBuilder.append(formattedAlterColumn);
            if(expected.isNullable() == false){
                stringBuilder.append(" NOT NULL");
            }
            stringBuilder.append(";");
            return stringBuilder.toString();
        }

        log.info("No action taken for changes detected in column: {}.{}.{}", change.getTable().getSchema(),
                change.getTable().getName(),
                expected.getName());
        return "";
    }

    @Override
    public String dropColumn(ColumnChange change) {
        Table table = change.getTable();
        Column actual = change.getActual();

        Map<String, Object> params = new HashMap<>();
        params.put("schemaName", table.getSchema());
        params.put("tableName", table.getName());
        params.put("columnName", actual.getName());
        return TemplateEngine.process(COLUMN_DROP_TEMPLATE, params);
    }

    @Override
    public String addColumn(ColumnChange change) {
        Table table = change.getTable();
        Column expected = change.getExpected();

        Map<String, Object> params = new HashMap<>();
        params.put("schemaName", table.getSchema());
        params.put("tableName", table.getName());
        params.put("columnDefinition", columnSQLDecoratorFactory.decoratorFor(expected).expressSQl());
        return TemplateEngine.process(COLUMN_ADD_TEMPLATE, params);
    }

    @Override
    public String dropTable(Table actual) {
        Map<String, Object> params = new HashMap<>();
        params.put("schemaName", actual.getSchema());
        params.put("tableName", actual.getName());
        return TemplateEngine.process(TABLE_DROP_TEMPLATE, params);
    }

    @Override
    public String alterForeignKey(ForeignKeyChange change) {
        return "";
    }

    @Override
    public String dropForeignKey(ForeignKey actual) {
        Map<String, Object> params = new HashMap<>();
        params.put("schemaName", actual.getSchema());
        params.put("tableName", actual.getTableName());
        params.put("foreignkeyName", actual.getName());
        return TemplateEngine.process(FOREIGN_KEY_DROP_TEMPLATE, params);
    }

    @Override
    public String alterTable(Table expected, Table actual) {
        boolean doesPKExist = actual.getColumns().stream().map(Column::isPrimaryKey).reduce((aBoolean, aBoolean2) -> aBoolean || aBoolean2).orElse(false);
        boolean doWeNeedToCreatePk = expected.getColumns().stream().map(Column::isPrimaryKey).reduce((aBoolean, aBoolean2) -> aBoolean || aBoolean2).orElse(false);

        StringBuilder stringBuilder = new StringBuilder("ALTER TABLE")
                .append(handleNullSchema(expected.getSchema(), expected.getName()));

        if (doesPKExist) {
            stringBuilder.append(" DROP PRIMARY KEY");
        }

        if (doWeNeedToCreatePk) {
            Optional<String> primaryKeysForTable = createPrimaryKeysForTable(expected);
            if (primaryKeysForTable.isPresent()) {
                if (doesPKExist) {
                    stringBuilder.append(",");
                }
                stringBuilder.append(" ADD ").append(primaryKeysForTable.get());
            }
        }

        stringBuilder.append(";");
        return stringBuilder.toString();
    }

    private Optional<String> createPrimaryKeysForTable(Table table) {
        List<String> primaryKeys = table
                .getColumns()
                .stream()
                .filter(Column::isPrimaryKey)
                .sorted((o1, o2) -> o1.getPrimaryKeySequenceId() < o2.getPrimaryKeySequenceId() ? -1 : 1)
                .map(pk -> String.format(DEFAULT_WRAPPER+"%s"+DEFAULT_WRAPPER, pk.getName()))
                .collect(Collectors.toList());

        if (primaryKeys.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of("PRIMARY KEY (" + String.join(", ", primaryKeys) + ")");
    }

    private Optional<String> foreignKeys(Table table) {
        String result = table.getColumns().stream()
            .filter(column -> column.getForeignKeys() != null && !column.getForeignKeys().isEmpty())
            .filter(column -> !checkColumnIsInterleaved(table, column))
            .map(this::createForeignKeys).collect(Collectors.joining());

        return result.isEmpty() ? Optional.empty() : Optional.of(result);
    }

    private boolean checkColumnIsInterleaved(Table table, Column column) {
        if (table.getInterleave() != null &&
            column.isPrimaryKey() &&
            table.getInterleave().getParentName().equals(column.getForeignKeys().get(0).getPrimaryTableName())) {
            return true;
        }

        return false;
    }

    //ALTER TABLE rosetta.contacts ADD CONSTRAINT contacts_fk FOREIGN KEY (contact_id) REFERENCES rosetta."user"(user_id);
    private String createForeignKeys(Column column) {
        return column.getForeignKeys().stream().map(this::createForeignKey).collect(Collectors.joining());
    }

    private Optional<String> createIndicesForTable(Table table) {
        String result = table
            .getIndices()
            .stream()
            .map(this::createIndex).collect(Collectors.joining());

        return result.isEmpty() ? Optional.empty() : Optional.of(result);
    }

    @Override
    public String createIndex(Index index) {
        if (index.getName().equals("PRIMARY_KEY") || index.getName().startsWith("IDX_")) {
            return "";
        }
        String createIndexStatement = "CREATE INDEX ";
        if (!index.getNonUnique()) {
            createIndexStatement = "CREATE UNIQUE INDEX ";
        }
        return createIndexStatement + index.getName() + " ON" + handleNullSchema(index.getSchema(), index.getTableName())
                + "(" + commaSeperatedColumns(index.getColumnNames()) + ");\r";
    }

    @Override
    public String dropIndex(Index index) {
        return "DROP INDEX " + index.getName() + ";\r";
    }

    @Override
    public String createView(View view, boolean dropViewIfExists) {
        StringBuilder builder = new StringBuilder();

        if (dropViewIfExists) {
            dropView(view);
            builder.append(dropView(view));
        }
        Map<String, Object> createParams = new HashMap<>();
        createParams.put("viewName", view.getName());
        createParams.put("viewCode", view.getCode());
        builder.append(TemplateEngine.process(VIEW_CREATE_TEMPLATE, createParams));

        return builder.toString();
    }

    @Override
    public String alterView(View expected, View actual) {
        Map<String, Object> createParams = new HashMap<>();
        createParams.put("viewName", expected.getName());
        createParams.put("viewCode", expected.getCode());
        return TemplateEngine.process(VIEW_ALTER_TEMPLATE, createParams);
    }

    @Override
    public String dropView(View actual) {
        Map<String, Object> params = new HashMap<>();
        params.put("viewName", actual.getName());
        params.put("viewCode", actual.getCode());
        return TemplateEngine.process(VIEW_DROP_TEMPLATE, params);
    }

    private String handleNullSchema(String schema, String tableName) {
        return ((schema == null || schema.isEmpty()) ? " " : (" "+ DEFAULT_WRAPPER + schema + DEFAULT_WRAPPER +".")) + DEFAULT_WRAPPER + tableName + DEFAULT_WRAPPER;
    }

    private String foreignKeyDeleteRuleSanitation(String deleteRule) {
        if (deleteRule == null || deleteRule.isEmpty()) {
            return "";
        }
        return " " + deleteRule + " ";
    }

    private String foreignKeyDeleteRule(ForeignKey foreignKey) {
        if (foreignKey.getDeleteRule() != null || !foreignKey.getDeleteRule().isEmpty()) {
            log.warn("Spanner does not support 'ON DELETE CASCADE' for foreign keys. Will be ignored.");
        }
        return "";
    }

    private String commaSeperatedColumns(List<String> columnNames) {
        return columnNames
            .stream()
            .map(it -> DEFAULT_WRAPPER + it + DEFAULT_WRAPPER)
            .collect(Collectors.joining(","));
    }
}
