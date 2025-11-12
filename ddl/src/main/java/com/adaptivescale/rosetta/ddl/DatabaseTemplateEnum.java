package com.adaptivescale.rosetta.ddl;

public enum DatabaseTemplateEnum {

    SCHEMA_CREATE("schema_create"),
    TABLE_CREATE("table_create"),
    TABLE_ALTER("table_alter"),
    TABLE_ALTER_DROP_PRIMARY_KEY("table_alter_drop_primary_key"),
    TABLE_ALTER_ADD_PRIMARY_KEY("table_alter_add_primary_key"),
    TABLE_DROP("table_drop"),
    FOREIGNKEY_CREATE("foreignkey_create"),
    FOREIGNKEY_DROP("foreignkey_drop"),
    COLUMN_ADD("column_add"),
    COLUMN_ALTER_TYPE("column_alter_type"),
    COLUMN_ALTER_NULL("column_alter_null"),
    COLUMN_DROP("column_drop");

    private String name;

    DatabaseTemplateEnum(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
