package com.homework.java.db.creators;

import com.homework.java.props.PropertyReader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgresCreator implements Creator {
    @Override
    public Connection createConnection() {
        try {
            return DriverManager.getConnection(PropertyReader.getStandardUrl("postgresql"),
                PropertyReader.getUser("postgresql"),
                PropertyReader.getPassword("postgresql"));
        } catch (SQLException e) {
            throw new RuntimeException("Exception: " + e.getMessage());
        }
    }
}
