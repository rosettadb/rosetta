package integration;

import com.adaptivescale.rosetta.common.DriverManagerDriverProvider;
import com.adaptivescale.rosetta.common.models.AssertTest;
import com.adaptivescale.rosetta.common.models.Database;
import com.adaptivescale.rosetta.common.models.test.Tests;
import com.adaptivescale.rosetta.ddl.DDLFactory;
import com.adaptivescale.rosetta.ddl.change.PostgresChangeFinder;
import com.adaptivescale.rosetta.ddl.change.model.Change;
import com.adaptivescale.rosetta.ddl.executor.DDLExecutor;
import com.adaptivescale.rosetta.test.assertion.AssertionSqlGenerator;
import com.adaptivescale.rosetta.test.assertion.DefaultAssertTestEngine;
import com.adaptivescale.rosetta.test.assertion.DefaultSqlExecution;
import com.adaptivescale.rosetta.test.assertion.generator.AssertionSqlGeneratorFactory;
import integration.helpers.GenericJDBCContainer;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

import static org.junit.Assert.*;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PostgresDDLIntegrationTest {

    private static String IMAGE = "sakiladb/postgres:latest";
    private static String USERNAME = "sakila";
    private static String DATABASE = "sakila";
    private static String SCHEMA = "public";
    private static String PASSWORD = "p_ssW0rd";
    private static String DB_TYPE = "postgres";
    private static String JDBC_URL = "jdbc:postgresql://localhost:{PORT}/sakila";
    private static String CLASS_NAME = "org.postgresql.Driver";
    private static int PORT = 5432;

    private static String DROP_VIEWS = "DROP VIEW actor_info,customer_list,film_list,nicer_but_slower_film_list,sales_by_film_category,sales_by_store,staff_list cascade;";
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

    private static String CREATE_ALL_TYPES_DDL = "CREATE TABLE IF NOT EXISTS test_types ( \n" +
            "test_lseg lseg, \n" +
            "test_int2 int2, \n" +
            "test__char _char, \n" +
            "test__int2 _int2, \n" +
            "test__varbit _varbit, \n" +
            "test_jsonb jsonb, \n" +
            "test_interval interval, \n" +
            "test__macaddr8 _macaddr8, \n" +
            "test__bpchar _bpchar, \n" +
            "test_bool bool, \n" +
            "test_bpchar bpchar, \n" +
            "test_interval_year_to_month interval year to month, \n" +
            "test_decimal decimal, \n" +
            "test_year year, \n" +
            "test_json json, \n" +
            "test__interval _interval, \n" +
            "test__uuid _uuid, \n" +
            "test__point _point, \n" +
            "test__jsonb _jsonb, \n" +
            "test__tsquery _tsquery, \n" +
            "test__bytea _bytea, \n" +
            "test_date date, \n" +
            "test_path path, \n" +
            "test_boolean boolean, \n" +
            "test__xml _xml, \n" +
            "test__varchar _varchar, \n" +
            "test__bit _bit, \n" +
            "test__pg_lsn _pg_lsn, \n" +
            "test_mpaa_rating mpaa_rating, \n" +
            "test_character character, \n" +
            "test_text text, \n" +
            "test_inet inet, \n" +
            "test_char char, \n" +
            "test_numeric numeric, \n" +
            "test_bigserial bigserial, \n" +
            "test__cidr _cidr, \n" +
            "test_character_varying character varying, \n" +
            "test_bit bit, \n" +
            "test__int4 _int4, \n" +
            "test_double_precision double precision, \n" +
            "test_varbit varbit, \n" +
            "test_tsquery tsquery, \n" +
            "test_int4 int4, \n" +
            "test__line _line, \n" +
            "test_nchar nchar, \n" +
            "test__txid_snapshot _txid_snapshot, \n" +
            "test__timestamp _timestamp, \n" +
            "test_timestamp_with_time_zone timestamp with time zone, \n" +
            "test_int int, \n" +
            "test_bytea bytea, \n" +
            "test_uuid uuid, \n" +
            "test_macaddr macaddr, \n" +
            "test__circle _circle, \n" +
            "test_point point, \n" +
            "test_interval_day_to_second interval day to second, \n" +
            "test_money money, \n" +
            "test__macaddr _macaddr, \n" +
            "test_float8 float8, \n" +
            "test_integer integer, \n" +
            "test__box _box, \n" +
            "test__float8 _float8, \n" +
            "test__text _text, \n" +
            "test_serial serial, \n" +
            "test_time time, \n" +
            "test__time _time, \n" +
            "test_xml xml, \n" +
            "test__tsvector _tsvector, \n" +
            "test_timestamptz timestamptz, \n" +
            "test_polygon polygon, \n" +
            "test__money _money, \n" +
            "test_float4 float4, \n" +
            "test__lseg _lseg, \n" +
            "test__int8 _int8, \n" +
            "test_timetz timetz, \n" +
            "test_smallserial smallserial, \n" +
            "test_txid_snapshot txid_snapshot, \n" +
            "test__float4 _float4, \n" +
            "test_timestamp timestamp, \n" +
            "test_real real, \n" +
            "test_line line, \n" +
            "test_bit_varying bit varying, \n" +
            "test_circle circle, \n" +
            "test_timestamp_without_time_zone timestamp without time zone, \n" +
            "test__bool _bool, \n" +
            "test__path _path, \n" +
            "test_int8 int8, \n" +
            "test_cidr cidr, \n" +
            "test_smallint smallint, \n" +
            "test_tsvector tsvector, \n" +
            "test_bigint bigint, \n" +
            "test__polygon _polygon, \n" +
            "test__numeric _numeric, \n" +
            "test_varchar varchar, \n" +
            "test_macaddr8 macaddr8, \n" +
            "test__date _date, \n" +
            "test__json _json, \n" +
            "test_box box \n" +
            ");";

    @Rule
    public static GenericJDBCContainer container = new GenericJDBCContainer(
            IMAGE, USERNAME,PASSWORD, DATABASE, SCHEMA, DB_TYPE, JDBC_URL, CLASS_NAME, PORT).generateContainer();


    @BeforeAll
    public static void beforeAll() {
        container.getContainer().start();
        System.out.println();
    }

    @Test
    @DisplayName("Prep Postgres SQL")
    @Order(0)
    void prep() throws Exception {
        container.getContainer().createConnection("").createStatement().execute(DROP_VIEWS);
        container.getContainer().createConnection("").createStatement().execute(CREATE_DDL);
        container.getContainer().createConnection("").createStatement().execute(CREATE_DDL1);
        container.getContainer().createConnection("").createStatement().execute(CREATE_DDL2);
        container.getContainer().createConnection("").createStatement().execute(CREATE_DDL3);
        container.getContainer().createConnection("").createStatement().execute(CREATE_DDL4);
        container.getContainer().createConnection("").createStatement().execute(CREATE_ALL_TYPES_DDL);
    }

    @Test
    @DisplayName("Test Postgres SQL extract is valid")
    @Order(1)
    void textExtract() throws Exception {
        Database sourceModel = container.getDatabaseModel();
        assertSame("Comparing table count.", 21, sourceModel.getTables().size());
        assertSame("Comparing actor table column count.", 4, container.getTableColumns(sourceModel, "actor").size());
    }

    @Test
    @DisplayName("Test Postgres SQL change finder")
    @Order(2)
    void testDiff() throws Exception {
        Database sourceModel = container.getDatabaseModel();
        ObjectMapper objectMapper = new ObjectMapper();
        Database targetModel = objectMapper.readValue(objectMapper.writeValueAsString(sourceModel), Database.class);
        targetModel.getTables().forEach(table -> {
            if(table.getName().equals("payment")){
                table.setName("payments");
            }
        });
        List<Change<?>> changes = new PostgresChangeFinder().findChanges(sourceModel, targetModel);
        assertSame("Total changes", changes.size(), 5);
        assertSame("Added table", changes.get(0).getStatus().toString(), "ADD");
        assertSame("Added table", changes.get(1).getStatus().toString(), "ADD");
        assertSame("Added table", changes.get(2).getStatus().toString(), "ADD");
        assertSame("Added table", changes.get(3).getStatus().toString(), "ADD");
        assertSame("Dropped table", changes.get(4).getStatus().toString(), "DROP");
    }


    @Test
    @DisplayName("Test Postgres SQL apply changes")
    @Order(3)
    void testApply() throws Exception {
        Database sourceModel = container.getDatabaseModel();
        ObjectMapper objectMapper = new ObjectMapper();
        Database targetModel = objectMapper.readValue(objectMapper.writeValueAsString(sourceModel), Database.class);
        targetModel.getTables().forEach(table -> {
            if(table.getName().equals("actor")){
                table.setName("actors");
            }
        });
        List<Change<?>> changes = DDLFactory.changeFinderForDatabaseType(DB_TYPE).findChanges(targetModel, sourceModel);
        String ddlForChanges = DDLFactory.changeHandler(sourceModel.getDatabaseType()).createDDLForChanges(changes);
        DDLExecutor executor = DDLFactory.executor(container.getRosettaConnection(), new DriverManagerDriverProvider());
        executor.execute(ddlForChanges);
        Database updatedModel = container.getDatabaseModel();
        long actors = updatedModel.getTables().stream().filter(table -> table.getName().equals("actors")).count();
        long actor = updatedModel.getTables().stream().filter(table -> table.getName().equals("actor")).count();
        assertSame("Actors table exists", 1L, actors);
        assertSame("Actor table is removed", 0L, actor);
    }

    @Test
    @DisplayName("Test Postgres SQL Assertion")
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
    @DisplayName("Extract PostgreSQL")
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
                        case "user1_salary":
                            assertEquals("", 1L, column.getForeignKeys().size());
                            break;
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
