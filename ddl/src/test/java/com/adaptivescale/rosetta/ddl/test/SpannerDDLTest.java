package com.adaptivescale.rosetta.ddl.test;

import com.adaptivescale.rosetta.common.models.Database;
import com.adaptivescale.rosetta.ddl.change.ChangeHandler;
import com.adaptivescale.rosetta.ddl.change.ChangeHandlerImplementation;
import com.adaptivescale.rosetta.ddl.change.DefaultChangeFinder;
import com.adaptivescale.rosetta.ddl.change.model.Change;
import com.adaptivescale.rosetta.ddl.targets.spanner.SpannerDDLGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class SpannerDDLTest {

    private static final Path resourceDirectory = Paths.get("src", "test", "resources", "ddl", "spanner_ddl");

    @Test
    public void addInterleaveTable() throws IOException {
        String ddl = generateDDL("add_interleave_table");
        Assertions.assertEquals("CREATE TABLE AlbumsNew(SingerId INT64 NOT NULL , AlbumId INT64 NOT NULL , AlbumTitle STRING(MAX)) PRIMARY KEY (SingerId, AlbumId),\n" +
                "INTERLEAVE IN PARENT Singers ON DELETE CASCADE;\n",  ddl.replaceAll("(?m)^[ \t]*\r?\n", ""));
    }

    @Test
    public void createDB() throws IOException {
    String ddl = generateDDL("clean_database");
    Assertions.assertEquals(" CREATE TABLE Singers(SingerId STRING(1024) NOT NULL , FirstName STRING(1024), LastName STRING(1024)) PRIMARY KEY (SingerId);  CREATE VIEW `SingerNames` SQL SECURITY INVOKER AS SELECT    Singers.SingerId AS SingerId,    Singers.FirstName || ' ' || Singers.LastName AS Name FROM Singers ;CREATE VIEW `NamesSinger` SQL SECURITY INVOKER AS SELECT    Singers.SingerId AS SingerId,    Singers.FirstName,   Singers.LastName FROM Singers ;",
                            ddl.replaceAll("(\\n)", " ").replaceAll("(\\r)", ""));
    }

    @Test
    public void addTable() throws IOException {
    String ddl = generateDDL("add_table");
    Assertions.assertEquals("CREATE TABLE Logs(LogId INT64 NOT NULL , Description STRING(1024)) PRIMARY KEY (LogId);\n",  ddl.replaceAll("(?m)^[ \t]*\r?\n", ""));
    }

    @Test
    public void dropTable() throws IOException {
    String ddl = generateDDL("drop_table");
    Assertions.assertEquals("DROP TABLE Logs;\n", ddl.replaceAll("(?m)^[ \t]*\r?\n", ""));
    }

    @Test
    public void addColumn() throws IOException {
    String ddl = generateDDL("add_column");
    Assertions.assertEquals("ALTER TABLE Logs ADD COLUMN Status STRING(1024);\n", ddl.replaceAll("(?m)^[ \t]*\r?\n", ""));
    }

    @Test
    public void dropColumn() throws IOException {
        String ddl = generateDDL("drop_column");
        Assertions.assertEquals("ALTER TABLE Logs DROP COLUMN Status;\n", ddl.replaceAll("(?m)^[ \t]*\r?\n", ""));
    }

    @Test
    public void alterColumnDataType() throws IOException {
        String ddl = generateDDL("alter_column_data_type");
        Assertions.assertEquals("ALTER TABLE Logs ALTER COLUMN Status STRING(1024);", ddl);
    }

    @Test
    public void alterColumnToNullable() throws IOException {
        String ddl = generateDDL("alter_column_to_nullable");
        Assertions.assertEquals("ALTER TABLE Logs ALTER COLUMN Status STRING(1024);", ddl);
    }

    @Test
    public void addView() throws IOException {
        String ddl = generateDDL("add_view");
        Assertions.assertEquals("CREATE VIEW `NamesSinger`\n" +
                              "SQL SECURITY INVOKER\n" +
                              "AS\n" +
                              "SELECT\n" +
                              "   Singers.SingerId AS SingerId,\n" +
                              "   Singers.FirstName,\n" +
                              "  Singers.LastName\n" +
                              "FROM Singers\n" +
                              ";",
                            ddl);
    }

    @Test
    public void dropView() throws IOException {
        String ddl = generateDDL("drop_view");
        Assertions.assertEquals("DROP VIEW `NamesSinger`;", ddl);
    }

    @Test
    public void alterView() throws IOException {
        String ddl = generateDDL("alter_view");
        Assertions.assertEquals("CREATE OR REPLACE VIEW `NamesSinger`\n" +
                                  "SQL SECURITY INVOKER\n" +
                              "AS\n" +
                              "SELECT\n" +
                              "   Singers.SingerId AS SingerId,\n" +
                              "   Singers.FirstName\n" +
                              "FROM Singers\n" +
                              ";", ddl);
    }

    private String generateDDL(String testType) throws IOException {
        Database actual = Utils.getDatabase(resourceDirectory.resolve(testType), "actual_model.yaml");
        Database expected = Utils.getDatabase(resourceDirectory.resolve(testType), "expected_model.yaml");
        List<Change<?>> changes = new DefaultChangeFinder().findChanges(expected, actual);
        ChangeHandler handler = new ChangeHandlerImplementation(new SpannerDDLGenerator(), null);
        return handler.createDDLForChanges(changes);
    }
}
