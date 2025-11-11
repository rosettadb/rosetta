package com.adaptivescale.rosetta.cli;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.adaptivescale.rosetta.cli.helpers.DriverHelper;
import com.adaptivescale.rosetta.cli.model.Config;
import com.adaptivescale.rosetta.cli.outputs.DbtSqlModelOutput;
import com.adaptivescale.rosetta.cli.outputs.DbtYamlModelOutput;
import com.adaptivescale.rosetta.cli.outputs.StringOutput;
import com.adaptivescale.rosetta.cli.outputs.YamlModelOutput;
import com.adaptivescale.rosetta.common.models.Database;
import com.adaptivescale.rosetta.common.DriverManagerDriverProvider;
import com.adaptivescale.rosetta.common.models.DriverInfo;
import com.adaptivescale.rosetta.common.models.Table;
import com.adaptivescale.rosetta.common.models.dbt.DbtModel;
import com.adaptivescale.rosetta.common.models.dbt.DbtSource;
import com.adaptivescale.rosetta.common.models.dbt.DbtTable;
import com.adaptivescale.rosetta.common.models.enums.OperationLevelEnum;
import com.adaptivescale.rosetta.common.models.input.Connection;
import com.adaptivescale.rosetta.common.types.DriverClassName;
import com.adaptivescale.rosetta.ddl.DDL;
import com.adaptivescale.rosetta.ddl.executor.DDLExecutor;
import com.adaptivescale.rosetta.ddl.DDLFactory;
import com.adaptivescale.rosetta.ddl.change.ChangeFinder;
import com.adaptivescale.rosetta.ddl.change.ChangeHandler;
import com.adaptivescale.rosetta.ddl.change.model.Change;
import com.adaptivescale.rosetta.ddl.utils.TemplateEngine;
import com.adaptivescale.rosetta.test.assertion.AssertionSqlGenerator;
import com.adaptivescale.rosetta.test.assertion.DefaultAssertTestEngine;
import com.adaptivescale.rosetta.test.assertion.DefaultSqlExecution;
import com.adaptivescale.rosetta.test.assertion.generator.AssertionSqlGeneratorFactory;
import com.adaptivescale.rosetta.diff.DiffFactory;
import com.adaptivescale.rosetta.diff.Diff;
import com.adaptivescale.rosetta.translator.Translator;
import com.adaptivescale.rosetta.translator.TranslatorFactory;
import com.adataptivescale.rosetta.source.core.SourceGeneratorFactory;

import com.adataptivescale.rosetta.source.dbt.DbtModelGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import queryhelper.pojo.GenericResponse;
import queryhelper.service.AIService;
import queryhelper.service.DbtAIService;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.adaptivescale.rosetta.cli.Constants.CONFIG_NAME;
import static com.adaptivescale.rosetta.cli.Constants.TEMPLATE_CONFIG_NAME;
import static com.adaptivescale.rosetta.cli.helpers.DbtEnhancedModelTransformer.enhancedSQLGenerator;

@CommandLine.Command(name = "cli",
        mixinStandardHelpOptions = true,
        version = "2.8.6",
        description = "Declarative Database Management - DDL Transpiler",
        subcommands = {
                com.adaptivescale.rosetta.cli.DbtCommands.class
        }
)
class Cli implements Callable<Void> {
    private static final Logger log = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    public static final String DEFAULT_MODEL_YAML = "model.yaml";
    public static final String DEFAULT_OUTPUT_DIRECTORY = "data";
    public static final String DEFAULT_DRIVERS_YAML = "drivers.yaml";
    private static final String RAW_LAYER = "raw";
    private static final String STAGING_LAYER = "staging";
    private static final String ENHANCED_LAYER = "enhanced";
    private static final String BUSINESS_LAYER = "business";

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-c", "--config"},
            converter = ConfigYmlConverter.class,
            defaultValue = CONFIG_NAME,
            description = "YAML config file. If none is supplied it will use main.conf in the current directory if it exists.")
    @Getter
    private Config config;

    @CommandLine.Option(names = {"-v", "--verbose"}, scope = CommandLine.ScopeType.INHERIT, description = "Enable Verbose output")
    public void setVerbose(boolean verbose) {
        if (verbose) {
            log.setLevel(Level.DEBUG);
        } else {
            log.setLevel(Level.INFO);
        }
    }

    @Override
    public Void call() {
        throw new CommandLine.ParameterException(spec.commandLine(), "Missing required subcommand");
    }

    @CommandLine.Command(name = "extract", description = "Extract schema chosen from connection config.", mixinStandardHelpOptions = true)
    private void extract(@CommandLine.Option(names = {"-s", "--source"}, required = true) String sourceName,
                         @CommandLine.Option(names = {"-t", "--convert-to"}) String targetName
    ) throws Exception {
        requireConfig(config);
        Connection source = getSourceConnection(sourceName);

        Path sourceWorkspace = Paths.get("./", sourceName);
        FileUtils.deleteDirectory(sourceWorkspace.toFile());
        Files.createDirectory(sourceWorkspace);

        Database result = SourceGeneratorFactory.sourceGenerator(source).generate(source);
        YamlModelOutput yamlInputModel = new YamlModelOutput(DEFAULT_MODEL_YAML, sourceWorkspace);
        yamlInputModel.write(result);
        log.info("Successfully written input database yaml ({}).", yamlInputModel.getFilePath());

        if (Optional.ofNullable(targetName).isEmpty()) {
            return;
        }

        Connection target = getTargetConnection(targetName);

        Path targetWorkspace = Paths.get("./", targetName);
        FileUtils.deleteDirectory(targetWorkspace.toFile());
        Files.createDirectory(targetWorkspace);

        generateTranslatedModels(source, sourceWorkspace, target, targetWorkspace);

        log.info("Successfully written output database yaml ({}/model.yml).", targetWorkspace);
    }

    @CommandLine.Command(name = "compile", description = "Generate DDL for target Database [bigquery, snowflake, …]", mixinStandardHelpOptions = true)
    private void compile(@CommandLine.Option(names = {"-s", "--source"}) String sourceName,
                         @CommandLine.Option(names = {"-t", "--target"}, required = true) String targetName,
                         @CommandLine.Option(names = {"-d", "--with-drop"}) boolean dropIfExist
    ) throws Exception {
        requireConfig(config);

        Connection target = getTargetConnection(targetName);

        Path targetWorkspace = Paths.get("./", targetName);
        List<FileNameAndDatabasePair> translatedModels;

        if (null == sourceName || sourceName.isBlank()) {
            if (!Files.isDirectory(targetWorkspace)) {
                throw new RuntimeException(String.format("Can not find directory: %s for target name: %s to find models" +
                        " for ddl generation", targetWorkspace, targetName));
            }
            translatedModels = getDatabases(targetWorkspace).collect(Collectors.toList());

            if (translatedModels.size() < 1) {
                throw new RuntimeException("Can not find any file with extension : .yaml." +
                        " Use extract command to generate models.");
            }
        } else {
            Connection source = getSourceConnection(sourceName);
            Path sourceWorkspace = Paths.get("./", sourceName);

            if (!Files.isDirectory(sourceWorkspace)) {
                throw new RuntimeException(String.format("Can not find directory: %s for source name: %s to find" +
                        " models for translation", sourceWorkspace, sourceName));
            }

            FileUtils.deleteDirectory(targetWorkspace.toFile());
            Files.createDirectory(targetWorkspace);

            translatedModels = generateTranslatedModels(source, sourceWorkspace, target, targetWorkspace);
        }

        String ddl = translatedModels.stream().map(stringDatabaseEntry -> {
            DDL modelDDL = DDLFactory.ddlForDatabaseType(stringDatabaseEntry.getValue().getDatabaseType());
            return modelDDL.createDatabase(stringDatabaseEntry.getValue(), dropIfExist);
        }).reduce("", (s, s2) -> s.concat("\n\n\n").stripLeading().concat(s2));

        StringOutput stringOutput = new StringOutput("ddl.sql", targetWorkspace);
        stringOutput.write(ddl);

        // generate dbt models
        generateStagingModels(target, targetWorkspace);

        log.info("Successfully written ddl ({}).", stringOutput.getFilePath());
    }

    @CommandLine.Command(name = "apply", description = "Get current model and compare with state of database," +
            " generate ddl for changes and apply to database. ", mixinStandardHelpOptions = true)
    private void apply(@CommandLine.Option(names = {"-s", "--source"}, required = true) String sourceName,
                       @CommandLine.Option(names = {"-m", "--model"}, defaultValue = DEFAULT_MODEL_YAML) String model) throws Exception {
        requireConfig(config);

        Connection source = getSourceConnection(sourceName);
        Path sourceWorkspace = Paths.get("./", sourceName);

        if (!Files.isDirectory(sourceWorkspace)) {
            throw new RuntimeException(String.format("Can not find directory: %s for source name: %s to find" +
                    " models for translation", sourceWorkspace, sourceName));
        }

        List<Database> databases = getDatabaseForModel(sourceWorkspace, model)
                .map(AbstractMap.SimpleImmutableEntry::getValue)
                .collect(Collectors.toList());

        if (databases.size() != 1) {
            throw new RuntimeException(String.format("For comparisons we need exactly one model. Found  %d models in" +
                    " directory %s", databases.size(), sourceWorkspace));
        }

        Database expectedDatabase = databases.get(0);
        Database actualDatabase = SourceGeneratorFactory.sourceGenerator(source).generate(source);

        if (expectedDatabase.getOperationLevel().equals(OperationLevelEnum.schema)) {
            Set<String> expectedSchemaList = expectedDatabase.getTables().stream().map(Table::getSchema).collect(Collectors.toSet());
            List<Table> tablesWithMatchingSchema = actualDatabase.getTables().stream().filter(table -> expectedSchemaList.contains(table.getSchema())).collect(Collectors.toList());
            actualDatabase.setTables(tablesWithMatchingSchema);
        }

        ChangeFinder changeFinder = DDLFactory.changeFinderForDatabaseType(source.getDbType());
        List<Change<?>> changes = changeFinder.findChanges(expectedDatabase, actualDatabase);

        if (changes.size() == 0) {
            log.info("No changes detected. Command aborted");
            return;
        }

        if (changes.stream().filter(change -> change.getStatus().equals(Change.Status.DROP)).findFirst().isPresent() &&
                expectedDatabase.getSafeMode()) {
            log.info("Not going to perform the changes because there are DROP operations and the safe mode is enabled.");
            return;
        }

        ChangeHandler handler = DDLFactory.changeHandler(source.getDbType());
        String ddl = handler.createDDLForChanges(changes);

        Path snapshotsPath = sourceWorkspace.resolve("snapshots");
        Path applyHistory = sourceWorkspace.resolve("apply");

        if (!Files.exists(snapshotsPath)) {
            Files.createDirectories(snapshotsPath);
        }

        if (!Files.exists(applyHistory)) {
            Files.createDirectories(applyHistory);
        }

        String timeStamp = new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date());
        String snapshotModelName = String.format("model-%s.yaml", timeStamp);
        String ddlHistoryName = String.format("ddl-%s.sql", timeStamp);

        YamlModelOutput yamlOutputModel = new YamlModelOutput(snapshotModelName, snapshotsPath);
        yamlOutputModel.write(actualDatabase);

        StringOutput stringOutput = new StringOutput(ddlHistoryName, applyHistory);
        stringOutput.write(ddl);

        DDLExecutor executor = DDLFactory.executor(source, new DriverManagerDriverProvider());
        executor.execute(ddl);

        if (config.isAutoCommit()) {
            gitCommandExecutor(sourceWorkspace.toString());
        }

        log.info("Successfully written ddl ({}).", stringOutput.getFilePath());
    }

    @CommandLine.Command(name = "test", description = "Run tests written on columns", mixinStandardHelpOptions = true)
    private void test(
            @CommandLine.Option(names = {"-s", "--source"}) String sourceName,
            @CommandLine.Option(names = {"-t", "--target"}) String targetName
    ) throws Exception {
        requireConfig(config);

        Optional<Connection> source = config.getConnection(sourceName);
        Optional<Connection> target = config.getConnection(targetName);

        if (source.isEmpty()) {
            throw new RuntimeException("Can not find source with name: " + sourceName + " configured in config.");
        }
        Path sourceWorkspace = Paths.get("./", sourceName);

        if (!Files.isDirectory(sourceWorkspace)) {
            throw new RuntimeException(String.format("Can not find directory: %s for source name: %s to find" +
                    " models for translation", sourceWorkspace, sourceName));
        }

        List<Database> collect = getDatabases(sourceWorkspace)
                .map(AbstractMap.SimpleImmutableEntry::getValue)
                .collect(Collectors.toList());
        for (Database database : collect) {
            AssertionSqlGenerator assertionSqlGenerator = AssertionSqlGeneratorFactory.generatorFor(source.get());
            DefaultSqlExecution defaultSqlExecution = new DefaultSqlExecution(source.get(), new DriverManagerDriverProvider());

            if (target.isEmpty()) {
                new DefaultAssertTestEngine(assertionSqlGenerator, defaultSqlExecution, null).run(source.get(), database);
            } else {
                DefaultSqlExecution targetSqlExecution = new DefaultSqlExecution(target.get(), new DriverManagerDriverProvider());
                new DefaultAssertTestEngine(assertionSqlGenerator, defaultSqlExecution, targetSqlExecution).run(source.get(), target.get(), database);
            }
        }
    }

    @CommandLine.Command(
            name = "init",
            description = "Creates a sample config (main.conf) and model directory.",
            mixinStandardHelpOptions = true
    )
    private void init(
            @CommandLine.Parameters(index = "0", description = "Project name.", defaultValue = "") String projectName,
            @CommandLine.Option(names = "--skip-db-selection", description = "Skip database selection and driver download process.") boolean skipDBSelection
        ) throws IOException {
        Path fileName = Paths.get(projectName, CONFIG_NAME);
        InputStream resourceAsStream = getClass().getResourceAsStream("/" + TEMPLATE_CONFIG_NAME);
        Path projectDirectory = Path.of(projectName);
        if (!projectName.isEmpty() && Files.isDirectory(projectDirectory)) {
            throw new RuntimeException(String.format("Project (%s) already exists.", projectName));
        }
        if (Files.exists(fileName)) {
            throw new RuntimeException("A configuration for this directory already exists.");
        }
        Files.createDirectories(projectDirectory);
        Files.copy(resourceAsStream, fileName);
        log.info("Successfully created project with a sample config ({}).", fileName);
        if (!projectName.isEmpty()) {
            log.info("In order to start using the newly created project please change your working directory.");
        }

        if (skipDBSelection) {
            log.info("Skipping database selection and driver download process.");
            return;
        }

        Path driversPath = Path.of(DEFAULT_DRIVERS_YAML);

        DriverHelper.printDrivers(driversPath);
        System.out.println("Please select the source database from the list above by typing its number (or press Enter to skip):");
        String sourceDB = new BufferedReader(new InputStreamReader(System.in)).readLine().trim();
        if (sourceDB.isEmpty()) {
            sourceDB = "skip";
        }
        handleDriverDownload(sourceDB, driversPath, "source");

        DriverHelper.printDrivers(driversPath);
        System.out.println("Please select the source database from the list above by typing its number (or press Enter to skip):");
        String targetDB = new BufferedReader(new InputStreamReader(System.in)).readLine().trim();
        if (targetDB.isEmpty()) {
            targetDB = "skip";
        }
        handleDriverDownload(targetDB, driversPath, "target");
    }

    private void handleDriverDownload(String dbChoice, Path driversPath, String dbType) {
        if ("skip".equalsIgnoreCase(dbChoice)) {
            log.info("Skipped downloading the {} DB driver.", dbType);
            return;
        }

        Integer driverId = null;

        try {
            driverId = Integer.parseInt(dbChoice);
        } catch (NumberFormatException e) {
            System.out.println("Invalid choice. Please select a valid option.");
            return;
        }

        List<DriverInfo> drivers = DriverHelper.getDrivers(driversPath);
        if (drivers.isEmpty()) {
            System.out.println("No drivers found in the specified YAML file.");
            return;
        }

        if (driverId < 1 || driverId > drivers.size()) {
            System.out.println("Invalid choice. Please select a valid option.");
            return;
        }

        DriverHelper.getDriver(driversPath, driverId);
    }

    @CommandLine.Command(name = "dbt", description = "Extract dbt models chosen from connection config.", mixinStandardHelpOptions = true)
    private void dbt(
            @CommandLine.Option(names = {"-s", "--source"}, required = true) String sourceName,
            @CommandLine.Option(names = {"--incremental"}, description = "Enable incremental mode for dbt model generation.", defaultValue = "false") boolean incremental,
            @CommandLine.Option(names = {"--business"}, description = "Create business layer models from enhanced models.", defaultValue = "false") boolean business,
            @CommandLine.Option(names = {"-q", "--query"}, required = false) String userPrompt) throws Exception {

        requireConfig(config);
        Connection source = getSourceConnection(sourceName);
        Path sourceWorkspace = Paths.get("./", sourceName);

        if (!Files.isDirectory(sourceWorkspace)) {
            throw new RuntimeException(String.format("Cannot find directory: %s for source name: %s to find models", sourceWorkspace, sourceName));
        }

        if (business) {
            createBusinessLayerModelsFromBestAvailable(source, sourceWorkspace, userPrompt);
            return;
        }

        if (incremental) {
            Path stagingWorkspace = sourceWorkspace.resolve("dbt").resolve("models").resolve("staging");
            if (!Files.exists(stagingWorkspace)) {
                log.info("Staging directory not found. You must create staging models first.");
                return;
            }
            List<Path> stagingSqlFiles;
            try (Stream<Path> sqlFiles = Files.list(stagingWorkspace)
                    .filter(path -> path.toString().endsWith(".sql"))) {
                stagingSqlFiles = sqlFiles.collect(Collectors.toList());
            }
            if (stagingSqlFiles.isEmpty()) {
                log.info("No SQL models found in staging layer. You must create staging models first.");
                return;
            }
            generateEnhancedModels(source, sourceWorkspace, stagingSqlFiles);
            return;
        }

        generateStagingModels(source, sourceWorkspace);
    }

    @CommandLine.Command(name = "generate", description = "Generate code", mixinStandardHelpOptions = true)
    private void generate(@CommandLine.Option(names = {"-s", "--source"}, required = true) String sourceName,
                          @CommandLine.Option(names = {"-t", "--target"}, required = true) String targetName,
                          @CommandLine.Option(names = {"--pyspark"}) boolean generateSpark,
                          @CommandLine.Option(names = {"--scala"}) boolean generateScala
    ) throws Exception {
        requireConfig(config);

        Connection source = getSourceConnection(sourceName);
        Connection target = getTargetConnection(targetName);

        Path sourceWorkspace = Paths.get("./", sourceName);
        if (!Files.exists(sourceWorkspace.toAbsolutePath()))
            Files.createDirectory(sourceWorkspace);

        if (generateSpark || !generateScala) {
            Database sourceDatabase = SourceGeneratorFactory.sourceGenerator(source).generate(source);
            String spark_code = generateSparkTemplateCode(source, target, sourceDatabase);
            StringOutput stringOutput = new StringOutput("spark_code.py", sourceWorkspace);
            stringOutput.write(spark_code);

            log.info("Successfully written spark code ({}).", stringOutput.getFilePath());
        }

        if (generateScala) {
            Database sourceDatabase = SourceGeneratorFactory.sourceGenerator(source).generate(source);
            String scala_code = generateScalaTemplateCode(source, target, sourceDatabase);
            StringOutput stringOutput = new StringOutput("scala_code.scala", sourceWorkspace);
            stringOutput.write(scala_code);

            log.info("Successfully written scala code ({}).", stringOutput.getFilePath());
        }
    }

    private String generateSparkTemplateCode(Connection source, Connection target, Database sourceDatabase) {
        Collection<Table> tables = sourceDatabase.getTables();

        Map<String, Object> variables = new HashMap<>();
        variables.put("sourceDataSource", source);
        variables.put("sourceDataSourceClassName", DriverClassName.valueOf(source.getDbType().toUpperCase()));
        variables.put("targetDataSource", target);
        variables.put("targetDataSourceClassName", DriverClassName.valueOf(target.getDbType().toUpperCase()));
        variables.put("tables", tables);

        return TemplateEngine.process("python/spark_code", variables);
    }

    private String generateScalaTemplateCode(Connection source, Connection target, Database sourceDatabase) {
        Collection<Table> tables = sourceDatabase.getTables();

        Map<String, Object> variables = new HashMap<>();
        variables.put("sourceDataSource", source);
        variables.put("sourceDataSourceClassName", DriverClassName.valueOf(source.getDbType().toUpperCase()));
        variables.put("targetDataSource", target);
        variables.put("targetDataSourceClassName", DriverClassName.valueOf(target.getDbType().toUpperCase()));
        variables.put("tables", tables);

        return TemplateEngine.process("scala/scala_code", variables);
    }

    /**
     * Determines the most advanced available layer and creates business models from it
     */
    private void createBusinessLayerModelsFromBestAvailable(Connection connection, Path sourceWorkspace, String userPrompt) throws IOException {
        Path dbtWorkspace = sourceWorkspace.resolve("dbt").resolve("models");
        String bestAvailableLayer = findBestAvailableLayer(dbtWorkspace, sourceWorkspace);
        log.info("Creating business layer models from {} layer", bestAvailableLayer.toUpperCase());

        List<String> modelContents = getModelContentsFromLayer(dbtWorkspace, bestAvailableLayer, sourceWorkspace);
        if (modelContents.isEmpty()) {
            throw new RuntimeException("No models found in any available layer to create business models from");
        }

        Path targetWorkspace = dbtWorkspace.resolve(BUSINESS_LAYER);
        Files.createDirectories(targetWorkspace);

        String combinedModelContents = String.join("\n\n", modelContents);

        // Check if we're generating from RAW layer to use appropriate prompt
        boolean isFromRawLayer = "raw".equalsIgnoreCase(bestAvailableLayer);

        GenericResponse response = DbtAIService.generateBusinessModels(
                config.getOpenAIApiKey(),
                config.getOpenAIModel(),
                targetWorkspace,
                combinedModelContents,
                userPrompt,
                isFromRawLayer  // Pass the flag to indicate if it's from RAW layer
        );

        if (response.getStatusCode() != 200) {
            throw new RuntimeException("Failed to generate business layer models: " + response.getMessage());
        }
    }

    /**
     * Finds the most advanced available layer in order: ENHANCED -> STAGING -> RAW
     */
    private String findBestAvailableLayer(Path dbtWorkspace, Path sourceWorkspace) {
        Path enhancedPath = dbtWorkspace.resolve(ENHANCED_LAYER);
        if (hasValidModels(enhancedPath, sourceWorkspace)) {
            return ENHANCED_LAYER;
        }

        Path stagingPath = dbtWorkspace.resolve(STAGING_LAYER);
        if (hasValidModels(stagingPath, sourceWorkspace)) {
            return STAGING_LAYER;
        }

        if (hasValidModels(null, sourceWorkspace)) {
            return RAW_LAYER;
        }


        throw new RuntimeException("No valid models found in any layer (raw, staging, enhanced)");
    }

    /**
     * Checks if a layer path exists and contains valid models
     */
    private boolean hasValidModels(Path layerPath, Path sourceWorkspace) {
        if (layerPath == null) {
            Path modelYamlPath = sourceWorkspace.resolve("model.yaml");
            return Files.exists(modelYamlPath);
        }

        if (!Files.exists(layerPath) || !Files.isDirectory(layerPath)) {
            return false;
        }

        // For other layers, check for SQL files
        try (Stream<Path> files = Files.list(layerPath)) {
            return files.anyMatch(path -> path.toString().endsWith(".sql"));
        } catch (IOException e) {
            log.warn("Error checking for models in path: {}", layerPath, e);
            return false;
        }
    }

    /**
     * Gets model contents from the specified layer
     */
    private List<String> getModelContentsFromLayer(Path dbtWorkspace, String layer, Path sourceWorkspace) throws IOException {
        List<String> modelContents = new ArrayList<>();
        Path layerPath = dbtWorkspace.resolve(layer);

        switch (layer) {
            case RAW_LAYER:
                Path modelYamlPath = sourceWorkspace.resolve("model.yaml");
                if (!Files.exists(modelYamlPath)) {
                    throw new RuntimeException("model.yaml not found in raw layer: " + modelYamlPath);
                }
                String yamlContent = new String(Files.readAllBytes(modelYamlPath));
                modelContents.add("model.yaml\n" + yamlContent);
                break;

            case ENHANCED_LAYER:
            case STAGING_LAYER:
                // For enhanced and staging layers, read SQL files directly
                Files.walk(layerPath)
                        .filter(Files::isRegularFile)
                        .filter(path -> path.toString().endsWith(".sql"))
                        .forEach(file -> {
                            try {
                                String content = new String(Files.readAllBytes(file));
                                modelContents.add(file.getFileName() + "\n" + content);
                            } catch (IOException e) {
                                log.error("Error reading file: {}", file, e);
                            }
                        });
                break;

            default:
                throw new IllegalArgumentException("Unknown layer: " + layer);
        }

        return modelContents;
    }

    private void generateEnhancedModels(Connection connection, Path sourceWorkspace, List<Path> stagingSqlFiles) throws IOException {
        Path dbtWorkspace = sourceWorkspace.resolve("dbt").resolve("models");
        Path enhancedWorkspace = dbtWorkspace.resolve("enhanced");

        Files.createDirectories(enhancedWorkspace);

        // Load database models
        List<Database> databases = getDatabases(sourceWorkspace)
                .map(AbstractMap.SimpleImmutableEntry::getValue)
                .collect(Collectors.toList());

        DbtModel dbtModel = DbtModelGenerator.dbtModelGenerator(databases);

//        DbtYamlModelOutput dbtYamlModelOutput = new DbtYamlModelOutput(enhancedWorkspace);
//        dbtYamlModelOutput.writeEnhanced(dbtModel);

        Map<String, String> enhancedSqlModels = enhancedSQLGenerator(stagingSqlFiles, null);

        DbtSqlModelOutput enhancedOutput = new DbtSqlModelOutput(enhancedWorkspace);
        enhancedOutput.write(enhancedSqlModels);

        log.info("Successfully written incremental dbt models based on existing staging models.");
    }

    private void generateStagingModels(Connection connection, Path sourceWorkspace) throws IOException {
        Path dbtWorkspace = sourceWorkspace.resolve("dbt").resolve("models");
        Path stagingWorkspace = dbtWorkspace.resolve("staging");

        Files.createDirectories(stagingWorkspace);

        List<Database> databases = getDatabases(sourceWorkspace)
                .map(AbstractMap.SimpleImmutableEntry::getValue)
                .collect(Collectors.toList());

        DbtModel dbtModel = DbtModelGenerator.dbtModelGenerator(databases);

        DbtYamlModelOutput dbtYamlModelOutput = new DbtYamlModelOutput(stagingWorkspace);
        dbtYamlModelOutput.write(dbtModel);

        Map<String, String> dbtSQLTables = DbtModelGenerator.dbtSQLGenerator(dbtModel, false);
        DbtSqlModelOutput dbtSqlModelOutput = new DbtSqlModelOutput(stagingWorkspace);
        dbtSqlModelOutput.write(dbtSQLTables);

        log.info("Successfully written staging dbt models for database yaml.");
    }

    @CommandLine.Command(name = "diff", description = "Show difference between local model and database", mixinStandardHelpOptions = true)
    private void diff(@CommandLine.Option(names = {"-s", "--source"}) String sourceName,
                      @CommandLine.Option(names = {"-m", "--model"}, defaultValue = DEFAULT_MODEL_YAML) String model) throws Exception {
        requireConfig(config);
        Connection sourceConnection = getSourceConnection(sourceName);

        Path sourceWorkspace = Paths.get("./", sourceName);
        if (!Files.isDirectory(sourceWorkspace)) {
            throw new RuntimeException(String.format("Can not find directory: %s for source name: %s to find" +
                    " models for translation", sourceWorkspace, sourceName));
        }

        List<Database> databases = getDatabaseForModel(sourceWorkspace, model)
                .map(AbstractMap.SimpleImmutableEntry::getValue)
                .collect(Collectors.toList());

        if (databases.size() != 1) {
            throw new RuntimeException(String.format("For comparisons we need exactly one model. Found  %d models in" +
                    " directory %s", databases.size(), sourceWorkspace));
        }

        Database localDatabase = databases.get(0);
        Database targetDatabase = SourceGeneratorFactory.sourceGenerator(sourceConnection).generate(sourceConnection);

        Diff<List<String>, Database, Database> tester = DiffFactory.diff(localDatabase.getDatabaseType());

        List<String> changeList = tester.find(localDatabase, targetDatabase);
        if (changeList.size() > 0) {
            System.out.println("There are changes between local model and targeted source");
            changeList.forEach(System.out::println);
        } else {
            System.out.println("There are no changes");
        }
    }

    @CommandLine.Command(name = "drivers", description = "Show available drivers for download", mixinStandardHelpOptions = true)
    private void drivers(@CommandLine.Option(names = {"--list"}, description = "Used to list all available drivers.") boolean isList,
                         @CommandLine.Option(names = {"--show"}, description = "Used to show downloaded drivers.") boolean isShow,
                         @CommandLine.Option(names = {"-dl", "--download"}, description = "Used to download selected driver by index.") boolean isDownload,
                         @CommandLine.Option(names = {"-f", "--file"}, description = "Used to change the drivers yaml file.", defaultValue = DEFAULT_DRIVERS_YAML) String file,
                         @CommandLine.Parameters(index = "0", arity = "0..1") Integer driverId) {
        Path driversPath = Path.of(file);

        if (isList) {
            DriverHelper.printDrivers(driversPath);

            System.out.println("To download a driver use: rosetta drivers {index} --download");
            System.out.println("To set a custom drivers path use ROSETTA_DRIVERS environment variable.");
            return;
        }

        if (isShow) {
            DriverHelper.printDownloadedDrivers();

            System.out.println("To download a driver use: rosetta drivers {index} --download");
            System.out.println("To set a custom drivers path use ROSETTA_DRIVERS environment variable.");
            return;
        }

        if (isDownload) {
            List<DriverInfo> drivers = DriverHelper.getDrivers(driversPath);
            if (drivers.isEmpty()) {
                System.out.println("No drivers found in the specified YAML file.");
                return;
            }

            if (driverId != null && driverId < 1 || driverId != null && driverId > drivers.size()) {
                System.out.println("Invalid index. Please provide a valid index.");
                return;
            }

            DriverHelper.getDriver(driversPath, driverId);
            return;
        }

        // if neither list nor download was called default to help command
        spec.subcommands().get("drivers").usage(System.out);
    }

    private void requireConfig(Config config) {
        if (config == null) {
            throw new RuntimeException("Config file is required.");
        }
    }

    private Connection getSourceConnection(String sourceName) {
        Optional<Connection> source = config.getConnection(sourceName);
        if (source.isEmpty()) {
            throw new RuntimeException(String.format("Can not find source with name: %s configured in config.",
                    sourceName));
        }
        return source.get();
    }

    private Connection getTargetConnection(String targetName) {
        Optional<Connection> target = config.getConnection(targetName);
        if (target.isEmpty()) {
            throw new RuntimeException("Can not find target with name: " + targetName + " configured in config.");
        }
        return target.get();
    }

    private List<FileNameAndDatabasePair> generateTranslatedModels(Connection source, Path sourceWorkspace, Connection target, Path targetWorkspace) throws IOException {
        List<FileNameAndDatabasePair> translatedModels;
        if (source.getDbType().equals(target.getDbType())) {
            log.info("Skipping translation because the target ({}) and source ({}) db types are the same.",
                    target.getDbType(), source.getDbType());
            translatedModels = getDatabases(sourceWorkspace).collect(Collectors.toList());
        } else {
            Translator<Database, Database> translator = TranslatorFactory.translator(source.getDbType(),
                    target.getDbType());

            translatedModels = getDatabases(sourceWorkspace)
                    .map(translateDatabases(translator))
                    .collect(Collectors.toList());
        }

        translatedModels.forEach(writeOutput(targetWorkspace));
        return translatedModels;
    }

    /**
     * @param directory directory same as connection name
     * @return consumer
     */
    private Consumer<AbstractMap.SimpleImmutableEntry<String, Database>> writeOutput(Path directory) {
        return fileNameAndDatabasePair -> {
            try {
                new YamlModelOutput(fileNameAndDatabasePair.getKey(), directory)
                        .write(fileNameAndDatabasePair.getValue());
            } catch (Exception e) {
                throw new RuntimeException(String.format("Unable to write translated models to path: %s", directory), e);
            }
        };
    }

    /**
     * Find every file that ends with .yaml
     *
     * @param directory where to search
     * @return Stream
     * @throws IOException exception with io
     */
    private Stream<FileNameAndDatabasePair> getDatabases(Path directory) throws IOException {
        return Files.list(directory)
                .filter(path -> !Files.isDirectory(path) && "yaml".equals(FilenameUtils.getExtension(path.toString())))
                .map(path -> {
                    try {
                        Database input = new ObjectMapper(new YAMLFactory()).readValue(path.toFile(), Database.class);
                        return new FileNameAndDatabasePair(path.getFileName().toString(), input);
                    } catch (Exception exception) {
                        throw new RuntimeException(exception);
                    }
                });
    }

    private Stream<FileNameAndDatabasePair> getDatabaseForModel(Path directory, String model) throws IOException {
        return Files.list(directory)
                .filter(path -> FilenameUtils.getName(path.toString()).equals(model) && !Files.isDirectory(path))
                .map(path -> {
                    try {
                        Database input = new ObjectMapper(new YAMLFactory()).readValue(path.toFile(), Database.class);
                        return new FileNameAndDatabasePair(path.getFileName().toString(), input);
                    } catch (Exception exception) {
                        throw new RuntimeException(exception);
                    }
                });
    }

    private Function<FileNameAndDatabasePair, FileNameAndDatabasePair> translateDatabases(Translator<Database, Database> translator) {
        return fileNameAndModelPair -> {
            try {
                Database translated = translator.translate(fileNameAndModelPair.getValue());
                return new FileNameAndDatabasePair(fileNameAndModelPair.getKey(), translated);
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        };
    }

    /**
     * Simple KeyValue Pair where key is file name and value is Database
     */
    private final static class FileNameAndDatabasePair extends AbstractMap.SimpleImmutableEntry<String, Database> {
        public FileNameAndDatabasePair(String key, Database value) {
            super(key, value);
        }
    }

    public void gitCommandExecutor(String sourceWorkspace) {
        File workspaceDir = new File(sourceWorkspace);
        File[] files = workspaceDir.listFiles((dir, name) -> name.endsWith(".yaml"));
        String commitMessage = "added: model yaml files";

        if (files == null || files.length == 0) {
            System.out.println("No YAML files found in the directory: " + sourceWorkspace);
            return;
        }

        Arrays.stream(files)
                .forEach(file -> executeGitCommand(new String[]{"git", "add", file.getAbsolutePath()}));

        executeGitCommand(new String[]{"git", "commit", "-m", commitMessage});

        if (config.getGitRemoteName() != "origin"){
            executeGitCommand(new String[]{"git", "push", config.getGitRemoteName(), "-u", "@"});
            return;
        }
        executeGitCommand(new String[]{"git", "push", "origin", "-u", "@"});

    }

    public static void executeGitCommand(String[] command) {
        try {
            Process process = new ProcessBuilder(command).start();

            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

            int exitCode = process.waitFor();

            if (exitCode == 0) {
                System.out.println("Git Commands have been executed successfully.");
                System.out.println("The YAML file has been pushed to your git branch ! ");
                return;
            }
            System.err.println("Error executing command: " + String.join(" ", command));

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }


    @CommandLine.Command(name = "query", description = "Query schema", mixinStandardHelpOptions = true)
    private void query(@CommandLine.Option(names = {"-s", "--source"}, required = true) String sourceName,
                       @CommandLine.Option(names = {"-q", "--query"}, required = true) String userQueryRequest,
                       @CommandLine.Option(names = {"-l", "--limit"}, required = false, defaultValue = "200") Integer showRowLimit,
                       @CommandLine.Option(names = {"--no-limit"}, required = false, defaultValue = "false") Boolean noRowLimit,
                       @CommandLine.Option(names = {"--output"}, required = false) Path output
    )
            throws Exception {
        requireConfig(config);

        if (config.getOpenAIApiKey() == null) {
            log.info("Open AI API key has to be provided in the config file");
            return;
        }

        Connection source = getSourceConnection(sourceName);

        Path sourceWorkspace = Paths.get("./", sourceName);

        Path dataDirectory = output != null ? output : sourceWorkspace.resolve(DEFAULT_OUTPUT_DIRECTORY);
        Path outputFile = dataDirectory.getFileName().toString().contains(".") ? dataDirectory.getFileName() : null;

        dataDirectory = dataDirectory.getFileName().toString().contains(".") ? dataDirectory.getParent() != null ? dataDirectory.getParent() : Paths.get(".") : dataDirectory;
        if (!dataDirectory.toFile().exists()) {
            Files.createDirectories(dataDirectory);
        }

        Database db = SourceGeneratorFactory.sourceGenerator(source).generate(source);

        DDL modelDDL = DDLFactory.ddlForDatabaseType(source.getDbType());
        String DDL = modelDDL.createDatabase(db, false);

        // If `noRowLimit` is true, set the row limit to 0 (no limit), otherwise use the value of `showRowLimit`
        GenericResponse response = AIService.generateQuery(userQueryRequest, config.getOpenAIApiKey(), config.getOpenAIModel(), DDL, source, noRowLimit ? 0 : showRowLimit, dataDirectory, outputFile);
        log.info(response.getMessage());
    }

    @CommandLine.Command(name = "validate", description = "Validate Connection", mixinStandardHelpOptions = true)
    private void validate(@CommandLine.Option(names = {"-s", "--source"}, required = true) String sourceName)
            throws Exception {

        requireConfig(config);
        Connection source = getSourceConnection(sourceName);
        Database database = SourceGeneratorFactory.sourceGenerator(source).validate(source);

        log.info("Successfully connected to {} through the configured source {}.", database.getDatabaseProductName(), source.getName());
    }

}
