package com.adaptivescale.rosetta.common;

import com.adaptivescale.rosetta.common.models.input.Connection;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DriverManagerDriverProvider implements JDBCDriverProvider {
    @Override
    public Driver getDriver(Connection connection) throws SQLException {
        return DriverManager.getDriver(connection.getUrl());
    }
}
