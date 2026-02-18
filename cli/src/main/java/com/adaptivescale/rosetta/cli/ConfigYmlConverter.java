package com.adaptivescale.rosetta.cli;

import com.adaptivescale.rosetta.cli.model.Config;
import com.adaptivescale.rosetta.common.models.input.Connection;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.text.StringSubstitutor;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;


public class ConfigYmlConverter implements CommandLine.ITypeConverter<Config> {
    @Override
    public Config convert(String value) throws Exception {
        File file = new File(value);
        if(!file.exists()){
            return null;
        }

        final String processedFileWithEnvParameters = processEnvParameters(file);
        return processConfigParameters(processedFileWithEnvParameters);
    }

    private String processEnvParameters(File file) throws IOException {
        String content = Files.readString(file.toPath());
        StringSubstitutor stringSubstitutor = new StringSubstitutor(System.getenv(), "${", "}");
        return stringSubstitutor.replace(content);
    }

    private Config processConfigParameters(String configContent) throws IOException {
        Config config = new ObjectMapper(new YAMLFactory()).readValue(configContent, Config.class);
        for (Connection connection : config.getConnections()) {
            Map<String, String> configParameters = connection.toMap();
            StringSubstitutor stringSubstitutor = new StringSubstitutor(configParameters, "${", "}");
            String processedUrl = stringSubstitutor.replace(connection.getUrl());
            connection.setUrl(processedUrl);
            
            // Process DuckLake-specific fields for environment variable substitution
            if (connection.getDuckdbDatabasePath() != null) {
                String processedDuckdbPath = stringSubstitutor.replace(connection.getDuckdbDatabasePath());
                connection.setDuckdbDatabasePath(processedDuckdbPath);
            }
            if (connection.getDucklakeDataPath() != null) {
                String processedDataPath = stringSubstitutor.replace(connection.getDucklakeDataPath());
                connection.setDucklakeDataPath(processedDataPath);
            }
            if (connection.getDucklakeMetadataDb() != null) {
                String processedMetadataDb = stringSubstitutor.replace(connection.getDucklakeMetadataDb());
                connection.setDucklakeMetadataDb(processedMetadataDb);
            }
            if (connection.getS3Region() != null) {
                connection.setS3Region(stringSubstitutor.replace(connection.getS3Region()));
            }
            if (connection.getS3AccessKeyId() != null) {
                connection.setS3AccessKeyId(stringSubstitutor.replace(connection.getS3AccessKeyId()));
            }
            if (connection.getS3SecretAccessKey() != null) {
                connection.setS3SecretAccessKey(stringSubstitutor.replace(connection.getS3SecretAccessKey()));
            }
        }

        return config;
    }
}
