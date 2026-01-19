package com.adaptivescale.rosetta.common.models.input;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class Connection {

    private String name;
    private String databaseName;
    private String schemaName;
    private String dbType;
    private String url;
    private String userName;
    private String password;
    private Collection<String> tables = new ArrayList<>();
    
    // DuckLake-specific fields
    private String duckdbDatabasePath;
    private String ducklakeDataPath;
    private String ducklakeMetadataDb;

    public Connection() {
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Collection<String> getTables() {
        return tables;
    }

    public void setTables(Collection<String> tables) {
        this.tables = tables;
    }

    public String getDuckdbDatabasePath() {
        return duckdbDatabasePath;
    }

    public void setDuckdbDatabasePath(String duckdbDatabasePath) {
        this.duckdbDatabasePath = duckdbDatabasePath;
    }

    public String getDucklakeDataPath() {
        return ducklakeDataPath;
    }

    public void setDucklakeDataPath(String ducklakeDataPath) {
        this.ducklakeDataPath = ducklakeDataPath;
    }

    public String getDucklakeMetadataDb() {
        return ducklakeMetadataDb;
    }

    public void setDucklakeMetadataDb(String ducklakeMetadataDb) {
        this.ducklakeMetadataDb = ducklakeMetadataDb;
    }

    public Map<String, String> toMap() {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.convertValue(this, Map.class);
    }
}
