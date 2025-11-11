package com.adaptivescale.rosetta.cli.services;

import com.adaptivescale.rosetta.cli.outputs.DbtSqlModelOutput;
import com.adaptivescale.rosetta.cli.outputs.DbtYamlModelOutput;
import com.adaptivescale.rosetta.common.models.dbt.DbtModel;
import com.adaptivescale.rosetta.common.models.input.Connection;
import com.adaptivescale.rosetta.common.models.Database;
import com.adataptivescale.rosetta.source.dbt.DbtModelGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.representer.Representer;
import org.yaml.snakeyaml.constructor.Constructor;
import queryhelper.pojo.GenericResponse;
import queryhelper.service.DbtAIService;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.adaptivescale.rosetta.cli.helpers.DbtEnhancedModelTransformer.enhancedSQLGenerator;

public class DbtModelService {

    private static final Logger log = LoggerFactory.getLogger(DbtModelService.class);

    public static final String STAGING_LAYER = "staging";
    public static final String ENHANCED_LAYER = "enhanced";
    public static final String BUSINESS_LAYER = "business";
    public static final String RAW_LAYER = "raw";

    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    private final RosettaService rosettaService = new RosettaService();

    public void generateDBTYamlModels(Connection connection, Path sourceWorkspace, String outputPath, boolean includeViews) throws Exception {
        rosettaService.extractModel(connection, sourceWorkspace);
        Path dbtModelsPath;

        if (outputPath != null && !outputPath.isEmpty()) {
            dbtModelsPath = Paths.get(outputPath);
        } else {
            dbtModelsPath = sourceWorkspace.resolve("dbt").resolve("models");
        }

        Files.createDirectories(dbtModelsPath);

        List<Database> databases = readYamlModels(sourceWorkspace);
        DbtModel dbtModel = DbtModelGenerator.dbtModelGenerator(databases, includeViews);

        new DbtYamlModelOutput(dbtModelsPath).write(dbtModel);
        log.info("Successfully written dbt models ({}).", dbtModelsPath);
    }

    public void generateStagingModels(Connection connection, Path sourceWorkspace, List<String> inputPaths, String outputPath) throws Exception {
        Path stagingPath = (outputPath != null && !outputPath.isEmpty())
                ? Paths.get(outputPath)
                : sourceWorkspace.resolve("dbt").resolve("models").resolve(STAGING_LAYER);

        Files.createDirectories(stagingPath);

        List<DbtModel> dbtModels;

        if (inputPaths != null && !inputPaths.isEmpty()) {
            dbtModels = readDbtModels(inputPaths);
        } else {
            dbtModels = readDbtModels(sourceWorkspace.resolve("dbt").resolve("models"));
        }

        if (dbtModels.isEmpty()) {
            log.warn("No DBT models found to generate staging models.");
            return;
        }

        Map<String, String> sqlModels = DbtModelGenerator.dbtSQLGenerator(dbtModels, false);
        new DbtSqlModelOutput(stagingPath).write(sqlModels);

        log.info("Written staging DBT models to {}", stagingPath);
    }

    public void generateEnhancedModels(Connection connection, Path sourceWorkspace, List<String> inputPaths, String outputPath, String prefix) throws IOException {
        Path enhancedPath;
        if (outputPath != null && !outputPath.isEmpty()) {
            enhancedPath = Paths.get(outputPath);
        } else {
            enhancedPath = sourceWorkspace.resolve("dbt").resolve("models").resolve(ENHANCED_LAYER);
        }
        Files.createDirectories(enhancedPath);

        List<Path> stagingSqlFiles;
        if (inputPaths != null && !inputPaths.isEmpty()) {
            stagingSqlFiles = getFilesFromPaths(inputPaths, Arrays.asList("sql"));
        } else {
            Path stagingPath = sourceWorkspace.resolve("dbt").resolve("models").resolve(STAGING_LAYER);
            stagingSqlFiles = listSqlFiles(stagingPath);
        }

        Map<String, String> enhancedSql = enhancedSQLGenerator(stagingSqlFiles, prefix);
        new DbtSqlModelOutput(enhancedPath).write(enhancedSql);

        log.info("Written enhanced DBT models to {}", enhancedPath);
    }

    public void generateBusinessModels(Connection connection, Path sourceWorkspace, String apiKey, String model, String userPrompt, List<String> inputPaths, String outputPath) throws IOException {
        Path businessPath;
        if (outputPath != null && !outputPath.isEmpty()) {
            businessPath = Paths.get(outputPath);
        } else {
            businessPath = sourceWorkspace.resolve("dbt").resolve("models").resolve(BUSINESS_LAYER);
        }
        Files.createDirectories(businessPath);

        List<String> sources;
        boolean isFromRawLayer = false;

        if (inputPaths != null && !inputPaths.isEmpty()) {
            sources = readModelContentsFromPaths(inputPaths);
            // Check if any input is from raw layer (yaml files)
            isFromRawLayer = inputPaths.stream().anyMatch(path ->
                    path.toLowerCase().endsWith(".yaml") || path.toLowerCase().endsWith(".yml"));
        } else {
            Path dbtModels = sourceWorkspace.resolve("dbt").resolve("models");
            String bestLayer = findBestAvailableLayer(dbtModels, sourceWorkspace);
            sources = readModelContents(dbtModels, bestLayer, sourceWorkspace);
            isFromRawLayer = "raw".equalsIgnoreCase(bestLayer);
        }

        if (sources.isEmpty()) {
            throw new RuntimeException("No valid model content found in any layer.");
        }

        GenericResponse response = DbtAIService.generateBusinessModels(
                apiKey, model, businessPath, String.join("\n\n", sources), userPrompt, isFromRawLayer
        );

        if (response.getStatusCode() != 200) {
            throw new RuntimeException("Failed to generate business models: " + response.getMessage());
        }

        log.info("Generated business models into {}", businessPath);
    }

    /**
     * Generate enhanced models directly from YAML files without persisting staging files
     */
    public void generateEnhancedModelsFromYaml(Connection connection, Path sourceWorkspace, List<String> yamlInputPaths, String outputPath, String prefix) throws IOException {
        Path enhancedPath;
        if (outputPath != null && !outputPath.isEmpty()) {
            enhancedPath = Paths.get(outputPath);
        } else {
            enhancedPath = sourceWorkspace.resolve("dbt").resolve("models").resolve(ENHANCED_LAYER);
        }
        Files.createDirectories(enhancedPath);

        List<DbtModel> dbtModels = readDbtModelYamls(sourceWorkspace, yamlInputPaths);

        // Generate enhanced SQL directly from each DBT model
        Map<String, String> enhancedSql = new HashMap<>();

        for (DbtModel dbtModel : dbtModels) {
            // Generate SQL models in memory for this DBT model
            Map<String, String> sqlModels = DbtModelGenerator.dbtSQLGenerator(dbtModel, false);

            // Generate enhanced SQL from the staging SQL content
            Map<String, String> modelEnhancedSql = enhancedSQLGenerator(sqlModels, prefix);

            // Add to the overall enhanced SQL map
            enhancedSql.putAll(modelEnhancedSql);
        }

        // Write all enhanced models to the output path
        new DbtSqlModelOutput(enhancedPath).write(enhancedSql);

        log.info("Written {} enhanced DBT models from YAML to {}", enhancedSql.size(), enhancedPath);
    }

    private List<DbtModel> readDbtModelYamls(Path sourceWorkspace, List<String> userInputPaths) throws IOException {
        List<Path> yamlFiles = new ArrayList<>();

        if (userInputPaths != null && !userInputPaths.isEmpty()) {
            yamlFiles = getFilesFromPaths(userInputPaths, Arrays.asList("yaml", "yml"));
        } else {
            Path dbtModelsPath = sourceWorkspace.resolve("dbt").resolve("models");
            if (!Files.exists(dbtModelsPath)) {
                throw new IOException("DBT models directory does not exist: " + dbtModelsPath);
            }

            // Find all YAML files in the dbt/models directory and subdirectories
            try (Stream<Path> paths = Files.walk(dbtModelsPath)) {
                yamlFiles = paths
                        .filter(Files::isRegularFile)
                        .filter(path -> {
                            String fileName = path.getFileName().toString().toLowerCase();
                            return fileName.endsWith(".yaml") || fileName.endsWith(".yml");
                        })
                        .collect(Collectors.toList());
            }
        }

        if (yamlFiles.isEmpty()) {
            throw new IOException("No YAML files found in the specified paths");
        }

        // Read each YAML file as a DbtModel
        List<DbtModel> dbtModels = new ArrayList<>();
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

        for (Path yamlFile : yamlFiles) {
            try {
                String content = Files.readString(yamlFile);
                DbtModel dbtModel = yamlMapper.readValue(content, DbtModel.class);
                dbtModels.add(dbtModel);
                log.debug("Successfully read DBT model from: {}", yamlFile);
            } catch (IOException e) {
                log.error("Failed to parse DBT model YAML file: {}", yamlFile, e);
                throw new IOException("Failed to parse DBT model YAML file: " + yamlFile, e);
            }
        }

        return dbtModels;
    }

    /**
     * Read YAML models from specific paths
     */
    private List<Database> readYamlModelsFromPaths(List<String> inputPaths, List<String> allowedExtensions) throws IOException {
        List<Path> yamlFiles = getFilesFromPaths(inputPaths, allowedExtensions);

        return yamlFiles.stream()
                .map(path -> {
                    try {
                        return yamlMapper.readValue(path.toFile(), Database.class);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to parse " + path, e);
                    }
                })
                .collect(Collectors.toList());
    }

    /**
     * Get files from input paths, filtering by allowed extensions
     */
    private List<Path> getFilesFromPaths(List<String> inputPaths, List<String> allowedExtensions) throws IOException {
        List<Path> result = new ArrayList<>();

        for (String inputPath : inputPaths) {
            Path path = Paths.get(inputPath);

            if (!Files.exists(path)) {
                log.warn("Path does not exist: {}", inputPath);
                continue;
            }

            if (Files.isDirectory(path)) {
                // If it's a directory, collect all files with allowed extensions
                try (Stream<Path> files = Files.walk(path)) {
                    List<Path> dirFiles = files
                            .filter(Files::isRegularFile)
                            .filter(p -> allowedExtensions.stream().anyMatch(ext ->
                                    p.toString().toLowerCase().endsWith("." + ext.toLowerCase())))
                            .collect(Collectors.toList());
                    result.addAll(dirFiles);
                }
            } else if (Files.isRegularFile(path)) {
                // If it's a file, check if it has an allowed extension
                String extension = FilenameUtils.getExtension(path.toString());
                if (allowedExtensions.stream().anyMatch(ext -> ext.equalsIgnoreCase(extension))) {
                    result.add(path);
                } else {
                    log.warn("File {} has unsupported extension. Allowed: {}", path, allowedExtensions);
                }
            }
        }

        return result;
    }

    /**
     * Read model contents from specified paths
     */
    private List<String> readModelContentsFromPaths(List<String> inputPaths) throws IOException {
        List<String> result = new ArrayList<>();
        List<String> allowedExtensions = Arrays.asList("sql", "yaml", "yml");

        List<Path> files = getFilesFromPaths(inputPaths, allowedExtensions);

        for (Path path : files) {
            try {
                String content = Files.readString(path);
                result.add(path.getFileName() + "\n" + content);
            } catch (IOException e) {
                log.error("Failed to read file: {}", path, e);
            }
        }

        return result;
    }

    public List<Database> readYamlModels(Path directory) throws IOException {
        return Files.list(directory)
                .filter(path -> FilenameUtils.getExtension(path.toString()).equalsIgnoreCase("yaml"))
                .map(path -> {
                    try {
                        return yamlMapper.readValue(path.toFile(), Database.class);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to parse " + path, e);
                    }
                })
                .collect(Collectors.toList());
    }

    public List<DbtModel> readDbtModels(Path directory) throws IOException {
        List<String> yamlFilePaths = Files.list(directory)
                .map(Path::toString)
                .filter(string -> FilenameUtils.getExtension(string).equalsIgnoreCase("yaml"))
                .collect(Collectors.toList());

        return readDbtModels(yamlFilePaths);
    }

    public List<DbtModel> readDbtModels(List<String> filePaths) throws IOException {
        List<DbtModel> result = new ArrayList<>();

        LoaderOptions loaderOptions = new LoaderOptions();
        loaderOptions.setMaxAliasesForCollections(Integer.MAX_VALUE);

        Yaml yaml = new Yaml(loaderOptions);
        for (String filePath : filePaths) {
            Object temp = yaml.load(new FileInputStream(filePath));
            result.add(yamlMapper.convertValue(temp, DbtModel.class));
        }
        return result;
    }

    public List<Path> listSqlFiles(Path directory) throws IOException {
        try (Stream<Path> paths = Files.list(directory)) {
            return paths.filter(path -> path.toString().endsWith(".sql"))
                    .collect(Collectors.toList());
        }
    }

    private String findBestAvailableLayer(Path dbtPath, Path sourcePath) {
        if (layerExists(dbtPath.resolve(ENHANCED_LAYER))) return ENHANCED_LAYER;
        if (layerExists(dbtPath.resolve(STAGING_LAYER))) return STAGING_LAYER;
        if (Files.exists(sourcePath.resolve("model.yaml"))) return RAW_LAYER;
        throw new RuntimeException("No valid model layer found (raw, staging, or enhanced).");
    }

    private boolean layerExists(Path path) {
        try (Stream<Path> stream = Files.exists(path) ? Files.list(path) : Stream.empty()) {
            return stream.anyMatch(p -> p.toString().endsWith(".sql"));
        } catch (IOException e) {
            return false;
        }
    }

    private List<String> readModelContents(Path dbtPath, String layer, Path sourcePath) throws IOException {
        List<String> result = new ArrayList<>();
        switch (layer) {
            case RAW_LAYER:
                Path rawModel = sourcePath.resolve("model.yaml");
                result.add("model.yaml\n" + Files.readString(rawModel));
                break;
            case STAGING_LAYER:
            case ENHANCED_LAYER:
                Path modelDir = dbtPath.resolve(layer);
                Files.walk(modelDir)
                        .filter(path -> path.toString().endsWith(".sql"))
                        .forEach(path -> {
                            try {
                                result.add(path.getFileName() + "\n" + Files.readString(path));
                            } catch (IOException e) {
                                log.error("Failed to read model file: {}", path, e);
                            }
                        });
                break;
            default:
                throw new IllegalArgumentException("Unknown layer: " + layer);
        }
        return result;
    }
}