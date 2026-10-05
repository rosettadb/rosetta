package com.adaptivescale.rosetta.common;

import com.adaptivescale.rosetta.common.models.input.Connection;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DriverManagerDriverProvider implements JDBCDriverProvider {
    @Override
    public Driver getDriver(Connection connection) throws SQLException {
        try {
            return DriverManager.getDriver(connection.getUrl());
        } catch (SQLException noDriverOnClasspath) {
            // Not on the classpath, look in the drivers directory
            Driver driver = DriverDirectoryLoader.findDriver(connection.getUrl());
            if (driver == null) {
                throw noDriverOnClasspath;
            }
            return driver;
        }
    }
}
