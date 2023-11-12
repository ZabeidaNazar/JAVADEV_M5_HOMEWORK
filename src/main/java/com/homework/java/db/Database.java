package com.homework.java.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class Database {
    private static final Database INSTANCE = new Database();
    private Map<DB, Connection> connections;
    private DB defaultDB;

    private Database() {
        connections = new HashMap<>();
        defaultDB = DB.POSTGRES;
    }

    public static Database getInstance() {
        return INSTANCE;
    }

    public void setDefaultDB(DB dbType) {
        defaultDB = dbType;
    }

    public DB getDefaultDB() {
        return defaultDB;
    }

    public Connection getConnection() {
        return getConnection(defaultDB);
    }

    public Connection getConnection(DB dbType) {
        if (connections.containsKey(dbType)) {
            return connections.get(dbType);
        }

        if (connections.isEmpty()) {
            defaultDB = dbType;
        }

        Connection connection = dbType.getConnection();
        connections.put(dbType, connection);
        return connection;
    }

    public static void close() {
        try {
            for (Connection connection : INSTANCE.connections.values()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void close(DB dbType) {
        try {
            Connection connection = INSTANCE.connections.get(dbType);
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
