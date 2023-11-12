package com.homework.java;

import com.homework.java.db.DB;
import com.homework.java.db.Database;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseInitService {
    public static void main(String[] args) {
        Database database = Database.getInstance();
        database.setDefaultDB(DB.POSTGRES);
        Connection connection = database.getConnection();
        try {
            Statement statement = connection.createStatement();
            for (String queries : SqlParser.getSqlCommands("sql/init_db.sql", 10)) {
                int i = statement.executeUpdate(queries);
                System.out.println("Updated rows: " + i);
            }
        } catch (SQLException e) {
            throw new RuntimeException("SQLException. Reason: " + e.getMessage());
        }
    }
}
