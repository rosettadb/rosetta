package com.adaptivescale.rosetta.cli;

import java.lang.instrument.Instrumentation;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Runs before Main (Premain-Class in the jar manifest, -javaagent in the launcher scripts) and adds
 * the JDBC driver jars to the classpath, which the launcher scripts used to do themselves.
 */
public class DriverAgent {
    private static final String DRIVERS_ENV = "ROSETTA_DRIVERS";
    private static final String DRIVERS_DIRECTORY = "drivers";

    public static void premain(String agentArgs, Instrumentation instrumentation) {
        try {
            Path directory = resolveDriverDirectory(System.getenv(DRIVERS_ENV));
            if (directory == null || !Files.isDirectory(directory)) {
                return;
            }

            List<Path> jars;
            try (Stream<Path> files = Files.walk(directory)) {
                jars = files
                        .filter(file -> file.getFileName().toString().toLowerCase().endsWith(".jar"))
                        .sorted()
                        .collect(Collectors.toList());
            }

            for (Path jar : jars) {
                try {
                    instrumentation.appendToSystemClassLoaderSearch(new JarFile(jar.toFile()));
                } catch (Throwable ignored) {
                    // not a readable jar, the classpath ignores those as well
                }
            }
        } catch (Throwable ignored) {
            // never prevent rosetta from starting, a missing driver is reported when it is needed
        }
    }

    /**
     * Resolves the directory where drivers are stored, the same one the launcher scripts create:
     * ROSETTA_DRIVERS when set, otherwise the drivers directory of the installation.
     *
     * @param driversEnv value of ROSETTA_DRIVERS, can be null
     * @return Path to the drivers directory or null if it can not be determined
     */
    static Path resolveDriverDirectory(String driversEnv) throws Exception {
        if (driversEnv != null && !driversEnv.isEmpty()) {
            // Remove any trailing '/*' or '/' from the path
            return Paths.get(driversEnv.replaceAll("[/\\\\]\\*$", "").replaceAll("[/\\\\]$", ""));
        }

        // <installation>/lib/cli.jar -> <installation>/drivers
        Path jarPath = Paths.get(DriverAgent.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        Path jarDirectory = jarPath.getParent();
        if (jarDirectory == null || jarDirectory.getParent() == null) {
            return null;
        }
        return jarDirectory.getParent().resolve(DRIVERS_DIRECTORY);
    }
}
