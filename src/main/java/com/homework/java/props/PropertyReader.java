package com.homework.java.props;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PropertyReader {
    private static final File appConfigPath = new File("src\\main\\resources\\app.properties");

    private static Properties getProperties() {
        Properties appProps = new Properties();
        try {
            appProps.load(new FileInputStream(appConfigPath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return appProps;
    }

    private static String getProperty(String name) {
        Properties appProps = getProperties();
        return appProps.getProperty(name);
    }

    public static String getStandardUrl(String dbName) {
        return new StringBuilder("jdbc:")
                .append(dbName)
                .append("://")
                .append(getHost(dbName))
                .append(":")
                .append(getPort(dbName))
                .append("/")
                .append(getDB(dbName))
                .toString();
    }

    public static String getHost(String dbName) {
        return getProperty(dbName + ".db.host");
    }

    public static String getPort(String dbName) {
        return getProperty(dbName + ".db.port");
    }

    public static String getDB(String dbName) {
        return getProperty(dbName + ".db.database");
    }

    public static String getUser(String dbName) {
        return getProperty(dbName + ".db.username");
    }

    public static String getPassword(String dbName) {
        return getProperty(dbName + ".db.password");
    }
}
