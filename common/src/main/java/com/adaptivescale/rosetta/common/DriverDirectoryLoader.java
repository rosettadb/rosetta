package com.adaptivescale.rosetta.common;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Loads JDBC drivers from the drivers directory instead of the classpath.
 * The directory is ROSETTA_DRIVERS when set, otherwise the drivers directory of the installation.
 */
public final class DriverDirectoryLoader {
    static final String DRIVERS_ENV = "ROSETTA_DRIVERS";
    static final String DRIVERS_DIRECTORY = "drivers";

    private static List<Driver> drivers;

    private DriverDirectoryLoader() {
    }

    /**
     * Finds a driver for the url among the drivers of the drivers directory.
     *
     * @param url JDBC url
     * @return the driver or null if none of them accepts the url
     */
    public static synchronized Driver findDriver(String url) {
        if (drivers == null) {
            drivers = loadDrivers(resolveDirectory(System.getenv(DRIVERS_ENV)));
        }
        for (Driver driver : drivers) {
            try {
                if (driver.acceptsURL(url)) {
                    return driver;
                }
            } catch (SQLException ignored) {
                // this driver can not handle the url, try the next one
            }
        }
        return null;
    }

    /**
     * Resolves the directory where drivers are stored.
     *
     * @param driversEnv value of ROSETTA_DRIVERS, can be null
     * @return the directory or null if it can not be determined
     */
    static Path resolveDirectory(String driversEnv) {
        if (driversEnv != null && !driversEnv.trim().isEmpty()) {
            // Remove any trailing '/*' or '/' from the path
            String driversPath = driversEnv.trim().replaceAll("[/\\\\]\\*$", "").replaceAll("[/\\\\]$", "");
            return Paths.get(driversPath);
        }
        try {
            // <installation>/lib/common.jar -> <installation>/drivers
            Path jarPath = Paths.get(DriverDirectoryLoader.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            Path jarDirectory = jarPath.getParent();
            if (jarDirectory == null || jarDirectory.getParent() == null) {
                return null;
            }
            return jarDirectory.getParent().resolve(DRIVERS_DIRECTORY);
        } catch (URISyntaxException | RuntimeException e) {
            return null;
        }
    }

    /**
     * Loads all drivers found in the .jar files of the directory and its subdirectories.
     * A driver that fails to load is skipped so it can not prevent the other drivers from loading.
     *
     * @param directory where to search, can be null
     * @return loaded drivers
     */
    static List<Driver> loadDrivers(Path directory) {
        if (directory == null || !Files.isDirectory(directory)) {
            return Collections.emptyList();
        }

        List<URL> jars = new ArrayList<>();
        try (Stream<Path> files = Files.walk(directory)) {
            List<Path> jarFiles = files
                    .filter(file -> file.getFileName().toString().endsWith(".jar"))
                    .sorted()
                    .collect(Collectors.toList());
            for (Path jarFile : jarFiles) {
                jars.add(jarFile.toUri().toURL());
            }
        } catch (IOException | RuntimeException e) {
            return Collections.emptyList();
        }
        if (jars.isEmpty()) {
            return Collections.emptyList();
        }

        URLClassLoader classLoader = new URLClassLoader(jars.toArray(new URL[0]), DriverDirectoryLoader.class.getClassLoader());
        // Drivers look up their own resources through the context class loader
        Thread.currentThread().setContextClassLoader(classLoader);

        List<Driver> loaded = new ArrayList<>();
        Iterator<Driver> iterator = ServiceLoader.load(Driver.class, classLoader).iterator();
        while (true) {
            try {
                if (!iterator.hasNext()) {
                    break;
                }
                loaded.add(iterator.next());
            } catch (Throwable ignored) {
                // broken driver jar, skip it and keep loading the rest
            }
        }
        return loaded;
    }
}
