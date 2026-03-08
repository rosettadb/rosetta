package com.adaptivescale.rosetta.common.models;

public class DatabaseTemplateModel {

    private Integer id;

    private String databaseType;

    private String templateName;

    private String templateFile;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getDatabaseType() {
        return databaseType;
    }

    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    public String getTemplateName() {
        return templateName;
    }

    public void setTemplateName(String templateName) {
        this.templateName = templateName;
    }

    public String getTemplateFile() {
        return templateFile;
    }

    public void setTemplateFile(String templateFile) {
        this.templateFile = templateFile;
    }

    public String generateInsertStatement(String tableName) {
        StringBuilder builder = new StringBuilder();
        builder.append("insert into ").append(tableName)
                .append(" values (")
                .append(id)
                .append(", '").append(databaseType).append("' ")
                .append(", '").append(templateName).append("' ")
                .append(", '").append(templateFile).append("' ")
                .append(");");
        return builder.toString();
    }

    @Override
    public String toString() {
        return "DatabaseTemplateModel{" +
                "id=" + id +
                ", databaseType='" + databaseType + '\'' +
                ", templateName='" + templateName + '\'' +
                ", templateFile='" + templateFile + '\'' +
                '}';
    }
}
