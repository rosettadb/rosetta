package integration;

import com.adaptivescale.rosetta.common.DriverManagerDriverProvider;
import com.adaptivescale.rosetta.common.models.AssertTest;
import com.adaptivescale.rosetta.common.models.Database;
import com.adaptivescale.rosetta.common.models.test.Tests;
import com.adaptivescale.rosetta.ddl.DDLFactory;
import com.adaptivescale.rosetta.ddl.change.RedshiftChangeFinder;
import com.adaptivescale.rosetta.ddl.change.model.Change;
import com.adaptivescale.rosetta.ddl.executor.DDLExecutor;
import com.adaptivescale.rosetta.test.assertion.AssertionSqlGenerator;
import com.adaptivescale.rosetta.test.assertion.DefaultAssertTestEngine;
import com.adaptivescale.rosetta.test.assertion.DefaultSqlExecution;
import com.adaptivescale.rosetta.test.assertion.generator.AssertionSqlGeneratorFactory;
import integration.helpers.GenericJDBCContainer;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

import static org.junit.Assert.*;

@Ignore
@Disabled
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RedshiftDDLIntegrationTest {

    private static String IMAGE = "ghcr.io/hearthsim/docker-pgredshift:latest";
    private static String USERNAME = "postgres";
    private static String DATABASE = "postgres";
    private static String SCHEMA = "public";
    private static String PASSWORD = "password";
    private static String DB_TYPE = "redshift";
    private static String JDBC_URL = "jdbc:postgresql://localhost:{PORT}/postgres";
    private static String CLASS_NAME = "org.postgresql.Driver";
    private static int PORT = 5432;

    private static String CREATE_DDL =
            "CREATE TABLE \"public\".numerics (\n" +
                    "\tc_bigint int8 NULL,\n" +
                    "\tc_bigserial bigserial NOT NULL,\n" +
                    "\tc_bytea bytea NULL,\n" +
                    "\tc_boolean bool NULL,\n" +
                    "\tc_bool bool NULL,\n" +
                    "\tc_integer int4 NULL,\n" +
                    "\tc_int int4 NULL,\n" +
                    "\tc_interval interval NULL,\n" +
                    "\tc_money money NULL,\n" +
                    "\tc_numeric numeric NULL,\n" +
                    "\tc_real float4 NULL,\n" +
                    "\tc_smallint int2 NULL,\n" +
                    "\tc_smallserial smallserial NOT NULL,\n" +
                    "\tc_serial serial4 NOT NULL,\n" +
                    "\tuser_salary money NOT NULL,\n" +
                    "\tCONSTRAINT numerics_pkey PRIMARY KEY (user_salary)\n" +
                    ");";

    private static String CREATE_DDL1 =
            "CREATE TABLE \"public\".strings (\n" +
                    "\tc_bit bit(1) NULL,\n" +
                    "\tc_bitvarying varbit NULL,\n" +
                    "\tc_character bpchar(1) NULL,\n" +
                    "\tc_charactervarying varchar NULL,\n" +
                    "\tc_text text NULL,\n" +
                    "\tc_varchar varchar(32) NULL,\n" +
                    "\tuser1_name varchar(32) NOT NULL,\n" +
                    "\tCONSTRAINT strings_pkey PRIMARY KEY (user1_name)\n" +
                    ");";

    private static String CREATE_DDL2 ="CREATE TABLE \"public\".time_date (\n" +
            "\tc_timestamp timestamp NULL,\n" +
            "\tc_timestamptz timestamptz NULL,\n" +
            "\tc_date date NULL,\n" +
            "\tc_time time NULL,\n" +
            "\tc_timetz timetz NULL,\n" +
            "\tuser_date date NOT NULL,\n" +
            "\tCONSTRAINT time_date_pkey PRIMARY KEY (user_date)\n" +
            ");";

    private static String CREATE_DDL3 ="CREATE TABLE \"public\".user1 (\n" +
            "\tcustomer_id int4 NULL,\n" +
            "\tuser1_salary money NULL,\n" +
            "\tuser_name varchar(32) NULL,\n" +
            "\tuser_date date NULL\n" +
            ");\n";
    private static String CREATE_DDL4 =
            "ALTER TABLE \"public\".user1 ADD CONSTRAINT user1_user1_salary_fkey FOREIGN KEY (user1_salary) REFERENCES \"public\".numerics(user_salary);\n" +
                    "ALTER TABLE \"public\".user1 ADD CONSTRAINT user1_user_date_fkey FOREIGN KEY (user_date) REFERENCES \"public\".time_date(user_date);\n" +
                    "ALTER TABLE \"public\".user1 ADD CONSTRAINT user1_user_name_fkey FOREIGN KEY (user_name) REFERENCES \"public\".strings(user1_name);";

    private static String CREATE_ALL_TYPES_DDL =
            "CREATE TABLE IF NOT EXISTS public.all_data_types (" +
            "    cint INTEGER," +
            "    cint2 SMALLINT," +
            "    cint8 BIGINT," +
            "    cdate DATE," +
            "    ctime TIME," +
            "    ctimestamp TIMESTAMP," +
            "    cnumeric NUMERIC(18)," +
            "    cfloat4 REAL," +
            "    cfloat8 DOUBLE PRECISION," +
            "    cbool BOOLEAN," +
            "    cchar CHAR," +
            "    cvarchar VARCHAR(256)" +
            ");";

    @Rule
    public static GenericJDBCContainer container = new GenericJDBCContainer(
            IMAGE, USERNAME,PASSWORD, DATABASE, SCHEMA, DB_TYPE, JDBC_URL, CLASS_NAME, PORT).withEnv("POSTGRES_PASSWORD", "password").generateContainer();


    @BeforeAll
    public static void beforeAll() {
        container.getContainer().start();
        System.out.println("");
    }

    @Test
    @DisplayName("Prep Redshift SQL")
    @Order(0)
    void prep() throws Exception {
        container.getContainer().createConnection("").createStatement().execute(CREATE_DDL);
        container.getContainer().createConnection("").createStatement().execute(CREATE_DDL1);
        container.getContainer().createConnection("").createStatement().execute(CREATE_DDL2);
        container.getContainer().createConnection("").createStatement().execute(CREATE_DDL3);
        container.getContainer().createConnection("").createStatement().execute(CREATE_DDL4);
        container.getContainer().createConnection("").createStatement().execute(CREATE_ALL_TYPES_DDL);
    }

    @Test
    @DisplayName("Test Redshift SQL extract is valid")
    @Order(1)
    void textExtract() throws Exception {
        Database sourceModel = container.getDatabaseModel();
        assertSame("Comparing table count.", 5, sourceModel.getTables().size());
        assertSame("Comparing actor table column count.", 7, container.getTableColumns(sourceModel, "strings").size());
    }

    @Test
    @DisplayName("Test Redshift SQL change finder")
    @Order(2)
    void testDiff() throws Exception {
        Database sourceModel = container.getDatabaseModel();
        ObjectMapper objectMapper = new ObjectMapper();
        Database targetModel = objectMapper.readValue(objectMapper.writeValueAsString(sourceModel), Database.class);
        targetModel.getTables().forEach(table -> {
            if(table.getName().equals("numerics")){
                table.setName("numeric");
            }
        });
        List<Change<?>> changes = new RedshiftChangeFinder().findChanges(sourceModel, targetModel);
        assertSame("Total changes", changes.size(), 2);
        assertSame("Added table", changes.get(0).getStatus().toString(), "ADD");
        assertSame("Dropped table", changes.get(1).getStatus().toString(), "DROP");
    }


    @Test
    @DisplayName("Test Redshift SQL apply changes")
    @Order(3)
    void testApply() throws Exception {
        Database sourceModel = container.getDatabaseModel();
        ObjectMapper objectMapper = new ObjectMapper();
        Database targetModel = objectMapper.readValue(objectMapper.writeValueAsString(sourceModel), Database.class);
        targetModel.getTables().forEach(table -> {
            if(table.getName().equals("all_data_types")){
                table.setName("all_data_types_updated");
            }
        });
        List<Change<?>> changes = DDLFactory.changeFinderForDatabaseType(DB_TYPE).findChanges(targetModel, sourceModel);
        String ddlForChanges = DDLFactory.changeHandler(sourceModel.getDatabaseType()).createDDLForChanges(changes);
        DDLExecutor executor = DDLFactory.executor(container.getRosettaConnection(), new DriverManagerDriverProvider());
        executor.execute(ddlForChanges);
        Database updatedModel = container.getDatabaseModel();
        long table_changed = updatedModel.getTables().stream().filter(table -> table.getName().equals("all_data_types_updated")).count();
        long table_old = updatedModel.getTables().stream().filter(table -> table.getName().equals("all_data_types")).count();
        assertSame("ChangedTable table exists", 1L, table_changed);
        assertSame("Old table is removed", 0L, table_old);
    }

    @Test
    @DisplayName("Test Redshift SQL Assertion")
    @Order(4)
    void testTest() throws Exception {
        Database sourceModel = container.getDatabaseModel();
        ObjectMapper objectMapper = new ObjectMapper();
        Database targetModel = objectMapper.readValue(objectMapper.writeValueAsString(sourceModel), Database.class);
        targetModel.getTables().forEach(table -> {
            if(table.getName().equals("actor")) {
                table.getColumns().forEach(column -> {
                    if(column.getName().equals("first_name")){
                        AssertTest assertTest = new AssertTest();
                        assertTest.setValue("Nick");
                        assertTest.setOperator("=");
                        assertTest.setExpected("1");
                        Tests tests = new Tests();
                        tests.setAssertions(List.of(assertTest));

                        column.setTests(tests);
                    }
                });
            }
        });

        AssertionSqlGenerator assertionSqlGenerator = AssertionSqlGeneratorFactory.generatorFor(container.getRosettaConnection());
        DefaultSqlExecution defaultSqlExecution = new DefaultSqlExecution(container.getRosettaConnection(), new DriverManagerDriverProvider());
        new DefaultAssertTestEngine(assertionSqlGenerator, defaultSqlExecution).run(container.getRosettaConnection(), targetModel);
    }

    @Test
    @DisplayName("Extract Redshift")
    @Order(5)

    void testExtractDDL() throws Exception {
        Database sourceModel = container.getDatabaseModel();
        ObjectMapper objectMapper = new ObjectMapper();
        Database targetModel = objectMapper.readValue(objectMapper.writeValueAsString(sourceModel), Database.class);
        targetModel.getTables().forEach(table ->{
            if (table.getName().equals("numerics")) {
                table.getColumns().forEach(column -> {
                    switch (column.getName()) {
                        case "c_integer":
                            assertEquals("", "int4", column.getTypeName());
                            break;
                        case "c_bigint":
                            assertEquals("", "int8", column.getTypeName());
                            break;
                        case "c_bigserial":
                            assertEquals("", "bigserial", column.getTypeName());
                            break;
                        case "c_bytea":
                            assertEquals("", "bytea", column.getTypeName());
                            break;
                        case "c_boolean":
                            assertEquals("", "bool", column.getTypeName());
                            break;
                        case "c_bool":
                            assertEquals("", "bool", column.getTypeName());
                            break;
                        case "c_int":
                            assertEquals("", "int4", column.getTypeName());
                            break;
                        case "c_interval":
                            assertEquals("", "interval", column.getTypeName());
                            break;
                        case "c_money":
                            assertEquals("", "money", column.getTypeName());
                            break;
                        case "c_numeric":
                            assertEquals("", "numeric", column.getTypeName());
                            break;
                        case "c_real":
                            assertEquals("", "float4", column.getTypeName());
                            break;
                        case "c_smallint":
                            assertEquals("", "int2", column.getTypeName());
                            break;
                        case "c_smallserial":
                            assertEquals("", "smallserial", column.getTypeName());
                            break;
                        case "c_serial":
                            assertEquals("", "serial", column.getTypeName());
                            break;
                        case "user_salary":
                            assertEquals("", "money", column.getTypeName());
                            break;
                    }
                });
            }
            if(table.getName().equals("strings")) {
                table.getColumns().forEach(column -> {
                    switch (column.getName()) {
                        case "c_bit":
                            assertEquals("", "bit", column.getTypeName());
                            break;
                        case "c_bitvarying":
                            assertEquals("", "varbit", column.getTypeName());
                            break;
                        case "c_character":
                            assertEquals("", "bpchar", column.getTypeName());
                            break;
                        case "c_charactervarying":
                            assertEquals("", "varchar", column.getTypeName());
                            break;
                        case "c_text":
                            assertEquals("", "text", column.getTypeName());
                            break;
                        case "c_varchar":
                            assertEquals("", "varchar", column.getTypeName());
                            break;
                        case "user1_name":
                            assertEquals("", "varchar", column.getTypeName());
                            assertEquals("", true, column.isPrimaryKey());
                            break;
                    }
                });
            }
            if (table.getName().equals("time_date")){
                table.getColumns().forEach(column -> {
                    switch (column.getName()) {
                        case "c_timestamp":
                            assertEquals("", "timestamp", column.getTypeName());
                            break;
                        case "c_timestamptz":
                            assertEquals("", "timestamptz", column.getTypeName());
                            break;
                        case "c_date":
                            assertEquals("", "date", column.getTypeName());
                            break;
                        case "c_time":
                            assertEquals("", "time", column.getTypeName());
                            break;
                        case "c_timetz":
                            assertEquals("", "timetz", column.getTypeName());
                            break;
                        case "user_date":
                            assertEquals("", "date", column.getTypeName());
                            assertEquals("", true, column.isPrimaryKey());
                            break;
                    }
                });
            }
        });

    }


    @Test
    @DisplayName("Foreign key constraint")
    @Order(6)
    void testForeignKey() throws Exception {
        Database sourceModel = container.getDatabaseModel();
        ObjectMapper objectMapper = new ObjectMapper();
        Database targetModel = objectMapper.readValue(objectMapper.writeValueAsString(sourceModel), Database.class);
        targetModel.getTables().forEach(table -> {
            if (table.getName().equals("user1")) {
                table.getColumns().forEach(column -> {
                    switch (column.getName()) {
                        case "user_name":
                            assertEquals("", 1L, column.getForeignKeys().size());
                            break;
                        case "user_date":
                            assertEquals("", 1L, column.getForeignKeys().size());
                            break;
                    }
                });
            }
        });
    }
}
