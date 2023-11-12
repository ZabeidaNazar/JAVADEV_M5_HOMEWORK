package com.homework.java.db.creators;

import com.homework.java.props.PropertyReader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MysqlCreator implements Creator {
    @Override
    public Connection createConnection() {
        try {
            return DriverManager.getConnection(PropertyReader.getStandardUrl("mysql") + "?allowMultiQueries=true",
                    PropertyReader.getUser("mysql"),
                    PropertyReader.getPassword("mysql"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
