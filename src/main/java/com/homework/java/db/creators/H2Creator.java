package com.homework.java.db.creators;

import com.homework.java.props.PropertyReader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class H2Creator implements Creator {
    @Override
    public Connection createConnection() {
        try { // jdbc:h2:D:\Goit_java\Dev\M4_HOMEWORK\h2\megasoft
            // jdbc:h2:~/test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH
            // jdbc:h2:tcp://localhost/~/test
            return DriverManager.getConnection("jdbc:h2:tcp://localhost/D:\\Goit_java\\Dev\\M4_HOMEWORK\\h2\\megasoft;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH",
                    PropertyReader.getUser("h2"),
                    PropertyReader.getPassword("h2"));
        } catch (SQLException e) {
            throw new RuntimeException("Exception: " + e.getMessage());
        }
    }
}
