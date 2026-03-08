package com.adaptivescale.rosetta.common;

import com.adaptivescale.rosetta.common.models.DatabaseTemplateModel;
import com.adaptivescale.rosetta.common.models.TranslationAttributeModel;
import com.adaptivescale.rosetta.common.models.TranslationModel;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.sql.*;
import java.util.*;

@Slf4j
public class TranslationMatrix {

    private static final String EXTERNAL_TRANSLATION_FILE_ENV = "EXTERNAL_TRANSLATION_FILE";
    private static final String EXTERNAL_TRANSLATION_ATTRIBUTE_FILE_ENV = "EXTERNAL_TRANSLATION_ATTRIBUTE_FILE";
    private static final String DEFAULT_TRANSLATION_MATRIX_FILE = "translation_matrix/translation.csv";
    private static final String DEFAULT_TRANSLATION_ATTRIBUTE_FILE = "translation_matrix/translation_attribute.csv";
    private static final String DELIMITER = ";;";
    private static final String URL = "jdbc:h2:mem:translation;DB_CLOSE_DELAY=-1";
    private static final String TRANSLATION_TABLE_NAME = "TRANSLATION";
    private static final String TRANSLATION_ATTRIBUTE_TABLE_NAME = "TRANSLATION_ATTRIBUTE";

    private static final String DATABASE_TEMPLATE_TABLE_NAME = "DATABASE_TEMPLATE";

    private static final String DEFAULT_DATABASE_TEMPLATE_FILE = "translation_matrix/database_template.csv";


    private static TranslationMatrix instance = null;

    public TranslationMatrix() {
        try {
            initTables();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static synchronized TranslationMatrix getInstance() {
        if(instance == null) {
            instance = new TranslationMatrix();
        }
        return instance;
    }

    void initTables() throws IOException {
        String translationTable = "CREATE TABLE "+ TRANSLATION_TABLE_NAME +"(id INT PRIMARY KEY AUTO_INCREMENT, " +
                "source_type VARCHAR(255) not null, " +
                "source_column_type VARCHAR(255) not null, " +
                "target_type VARCHAR(255) not null, " +
                "target_column_type VARCHAR(255) not null" +
                ");";
        String translationAttributesTable = "CREATE TABLE "+TRANSLATION_ATTRIBUTE_TABLE_NAME+"(id INT PRIMARY KEY AUTO_INCREMENT, " +
                "translation_id INT not null REFERENCES "+ TRANSLATION_TABLE_NAME +"(id), " +
                "attribute_name VARCHAR(255) not null, " +
                "attribute_value VARCHAR(255) not null" +
                ");";
        execute(translationTable);
        execute(translationAttributesTable);

        String index = "CREATE INDEX source_translation_index " +
                "ON "+ TRANSLATION_TABLE_NAME +" (source_type, source_column_type)";
        String uniqueIndex = "CREATE UNIQUE INDEX unique_translation_index " +
                "ON "+ TRANSLATION_TABLE_NAME +" (source_type, source_column_type, target_type)";
        execute(index);
        execute(uniqueIndex);

        String databaseTemplateTable = "CREATE TABLE "+ DATABASE_TEMPLATE_TABLE_NAME +"(id INT PRIMARY KEY AUTO_INCREMENT, " +
                "database_type VARCHAR(255) not null, " +
                "template_name VARCHAR(255) not null, " +
                "template_file VARCHAR(255) not null " +
                ");";
        execute(databaseTemplateTable);

        loadCSVData();
    }

    void loadCSVData() throws IOException {
        String line = "";

        StringBuilder dataInsertQuery = new StringBuilder();
        StringBuilder attributesInsertQuery = new StringBuilder();
        StringBuilder databaseTemplateInsertQuery = new StringBuilder();

        readDatabaseTemplate(databaseTemplateInsertQuery);
        Map<Integer, List<TranslationAttributeModel>> translationAttributesMappedByTranslationId = readTranslationAttributes(attributesInsertQuery);
        BufferedReader br = readTranslationMatrixFile();

        while ((line = br.readLine()) != null)
        {
            String[] translation = line.split(DELIMITER);
            TranslationModel translationModel = new TranslationModel();
            translationModel.setId(Integer.valueOf(translation[0]));
            translationModel.setSourceType(translation[1]);
            translationModel.setSourceColumnType(translation[2]);
            translationModel.setTargetType(translation[3]);
            translationModel.setTargetColumnType(translation[4]);
            translationModel.setAttributes(translationAttributesMappedByTranslationId.get(translationModel.getId()));

            String insertStatement = translationModel.generateInsertStatement(TRANSLATION_TABLE_NAME);
            dataInsertQuery.append(insertStatement);
        }
        execute(dataInsertQuery.toString());
        execute(attributesInsertQuery.toString());
        execute(databaseTemplateInsertQuery.toString());
    }

    void execute(String sql) {
        try {
            Connection connection = DriverManager.getConnection(URL);
            Statement statement = connection.createStatement();
            statement.execute(sql);
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    TranslationModel getSingleRecord(String query) {
        try {
            Connection connection = DriverManager.getConnection(URL);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);

            if (resultSet.next()) {
                TranslationModel translationModel = new TranslationModel();
                translationModel.setId(resultSet.getInt(1));
                translationModel.setSourceType(resultSet.getString(2));
                translationModel.setSourceColumnType(resultSet.getString(3));
                translationModel.setTargetType(resultSet.getString(4));
                translationModel.setTargetColumnType(resultSet.getString(5));
                return translationModel;
            }
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    private List<TranslationAttributeModel> getTranslationAttributeRecords(String query) {
        try {
            Connection connection = DriverManager.getConnection(URL);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            List<TranslationAttributeModel> result = new ArrayList<>();

            while (resultSet.next()) {
                TranslationAttributeModel translationAttributeModel = new TranslationAttributeModel();
                translationAttributeModel.setId(resultSet.getInt("id"));
                translationAttributeModel.setTranslationId(resultSet.getInt("translation_id"));
                translationAttributeModel.setAttributeName(resultSet.getString("attribute_name"));
                translationAttributeModel.setAttributeValue(resultSet.getString("attribute_value"));

                result.add(translationAttributeModel);
            }
            connection.close();
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Map<String, String>> getDatabaseTemplates(String query) {
        try {
            Connection connection = DriverManager.getConnection(URL);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            Map<String, Map<String, String>> result = new HashMap<>();

            while (resultSet.next()) {
                DatabaseTemplateModel databaseTemplateModel = new DatabaseTemplateModel();
                databaseTemplateModel.setId(resultSet.getInt("id"));
                databaseTemplateModel.setDatabaseType(resultSet.getString("database_type"));
                databaseTemplateModel.setTemplateName(resultSet.getString("template_name"));
                databaseTemplateModel.setTemplateFile(resultSet.getString("template_file"));

                if (!result.containsKey(databaseTemplateModel.getDatabaseType())) {
                    result.put(databaseTemplateModel.getDatabaseType(), new HashMap<>());
                }

                result.get(databaseTemplateModel.getDatabaseType())
                        .put(databaseTemplateModel.getTemplateName(), databaseTemplateModel.getTemplateFile());
            }

            connection.close();

            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public TranslationModel findById(Integer id) {
        String query = String.format("SELECT TOP 1 * from %s where id=%s", TRANSLATION_TABLE_NAME, id);
        return getSingleRecord(query);
    }

    public String findBySourceTypeAndSourceColumnType(String sourceType, String sourceColumnType) {
        String query = String.format("SELECT * from %s where source_type='%s' and source_column_type='%s' and target_type='%s'", TRANSLATION_TABLE_NAME, sourceType, sourceColumnType, sourceType);
        TranslationModel translationModel = getSingleRecord(query);
        return translationModel != null ? translationModel.getTargetColumnType() : sourceColumnType;
    }

    public TranslationModel findBySourceTypeAndSourceColumnTypeAndTargetType(String sourceType, String sourceColumnType, String targetType) {
        String query = String.format("SELECT * from %s where source_type='%s' and source_column_type='%s' and target_type='%s'",
                TRANSLATION_TABLE_NAME, sourceType, sourceColumnType.toLowerCase(), targetType);
        return getSingleRecord(query);
    }

    public List<TranslationAttributeModel> findByTranslationAttributesByTranslationIds(Integer translationId) {
        String query = String.format("SELECT * from %s where translation_id=%s",
                TRANSLATION_ATTRIBUTE_TABLE_NAME, translationId);
        return getTranslationAttributeRecords(query);
    }

    public Map<String, Map<String, String>> findDatabaseTemplates() {
        String query = String.format("SELECT * from %s",
                DATABASE_TEMPLATE_TABLE_NAME);

        return getDatabaseTemplates(query);
    }

    private Map<Integer, List<TranslationAttributeModel>> readTranslationAttributes(StringBuilder attributesInsertQuery) throws IOException {
        Map<Integer, List<TranslationAttributeModel>> translationAttributesMappedByTranslationId = new HashMap<>();
        BufferedReader br = readTranslationAttributesFile();
        String line = "";
        while ((line = br.readLine()) != null)
        {
            String[] translation = line.split(DELIMITER);
            TranslationAttributeModel translationAttributeModel = new TranslationAttributeModel();
            translationAttributeModel.setId(Integer.valueOf(translation[0]));
            translationAttributeModel.setTranslationId(Integer.valueOf(translation[1]));
            translationAttributeModel.setAttributeName(translation[2]);
            translationAttributeModel.setAttributeValue(translation[3]);
            if (translationAttributesMappedByTranslationId.containsKey(translationAttributeModel.getTranslationId())) {
                translationAttributesMappedByTranslationId.get(translationAttributeModel.getTranslationId()).add(translationAttributeModel);
            } else {
                translationAttributesMappedByTranslationId.put(translationAttributeModel.getTranslationId(), new ArrayList<>(Arrays.asList(translationAttributeModel)));
            }
            String insertStatement = translationAttributeModel.generateInsertStatement(TRANSLATION_ATTRIBUTE_TABLE_NAME);
            attributesInsertQuery.append(insertStatement);
        }
        return translationAttributesMappedByTranslationId;
    }

    private void readDatabaseTemplate(StringBuilder databaseTemplateInsertQuery) throws IOException {
        BufferedReader br = readDatabaseTemplateFile();
        String line = "";
        while ((line = br.readLine()) != null)
        {
            String[] translation = line.split(DELIMITER);
            DatabaseTemplateModel databaseTemplateModel = new DatabaseTemplateModel();
            databaseTemplateModel.setId(Integer.valueOf(translation[0]));
            databaseTemplateModel.setDatabaseType(translation[1]);
            databaseTemplateModel.setTemplateName(translation[2]);
            databaseTemplateModel.setTemplateFile(translation[3]);

            String insertStatement = databaseTemplateModel.generateInsertStatement(DATABASE_TEMPLATE_TABLE_NAME);
            databaseTemplateInsertQuery.append(insertStatement);
        }
    }

    private BufferedReader readTranslationMatrixFile() throws FileNotFoundException {
        //Check for the translation file from the ENV variable EXTERNAL_TRANSLATION_FILE
        String externalTranslationFile = System.getenv(EXTERNAL_TRANSLATION_FILE_ENV);
        if (externalTranslationFile != null) {
            File translationFile = new File(externalTranslationFile);
            InputStream targetStream = new FileInputStream(translationFile);
            return new BufferedReader(new InputStreamReader(targetStream));
        }

        //If the file is not provided in the project read it from the resources
        InputStream resourceAsStream = TranslationMatrix.class.getClassLoader().getResourceAsStream(DEFAULT_TRANSLATION_MATRIX_FILE);
        return new BufferedReader(new InputStreamReader(resourceAsStream));
    }

    private BufferedReader readTranslationAttributesFile() throws FileNotFoundException {
        //Check for the translation file from the ENV variable EXTERNAL_TRANSLATION_FILE
        String externalTranslationFile = System.getenv(EXTERNAL_TRANSLATION_ATTRIBUTE_FILE_ENV);
        if (externalTranslationFile != null) {
            File translationFile = new File(externalTranslationFile);
            InputStream targetStream = new FileInputStream(translationFile);
            return new BufferedReader(new InputStreamReader(targetStream));
        }

        //If the file is not provided in the project read it from the resources
        InputStream resourceAsStream = TranslationMatrix.class.getClassLoader().getResourceAsStream(DEFAULT_TRANSLATION_ATTRIBUTE_FILE);
        return new BufferedReader(new InputStreamReader(resourceAsStream));
    }

    private BufferedReader readDatabaseTemplateFile() throws FileNotFoundException {
        //If the file is not provided in the project read it from the resources
        InputStream resourceAsStream = TranslationMatrix.class.getClassLoader().getResourceAsStream(DEFAULT_DATABASE_TEMPLATE_FILE);
        return new BufferedReader(new InputStreamReader(resourceAsStream));
    }
}
