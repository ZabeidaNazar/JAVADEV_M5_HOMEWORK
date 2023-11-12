package com.homework.java;

import com.homework.java.db.Database;
import com.homework.java.selects.*;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class DatabaseQueryService {
    public static <R> List<R> selectQuery(String pathToSqlFile, SQLFunction<ResultSet, R> work) {
        Connection connection = Database.getInstance().getConnection();
        try {
            Statement statement = connection.createStatement();
            String queries = SqlParser.getSqlCommands(pathToSqlFile, 1).get(0);
            ResultSet resultSet = statement.executeQuery(queries);

            List<R> list = new ArrayList<>();
            while (resultSet.next()) {
                list.add(work.apply(resultSet));
            }

            statement.close();
            return list;
        } catch (SQLException e) {
            throw new RuntimeException("SQLException. \nReason: " + e.getMessage());
        }
    }

    public static List<MaxProjectCountClient> findMaxProjectsClient() {
        return selectQuery("sql/find_max_projects_client.sql", (resultSet) -> new MaxProjectCountClient(
                new String(resultSet.getString("name").getBytes(StandardCharsets.UTF_8)),
                resultSet.getInt("projects_count")));
    }

    public static List<MaxProjectCountWorker> findMaxProjectsWorker() {
        return selectQuery("sql/find_max_projects_worker.sql", (resultSet) -> new MaxProjectCountWorker(
                new String(resultSet.getString("name").getBytes(StandardCharsets.UTF_8)),
                resultSet.getInt("projects_count")));
    }

    public static List<LongestProject> findLongestProject() {
        return selectQuery("sql/find_longest_project.sql", (resultSet) -> new LongestProject(
                new String(resultSet.getString("name").getBytes(StandardCharsets.UTF_8), Charset.defaultCharset()),
                resultSet.getInt("month_count")));
    }

    public static List<MaxSalaryWorker> findMaxSalaryWorker() {
        return selectQuery("sql/find_max_salary_worker.sql", (resultSet) -> new MaxSalaryWorker(
                new String(resultSet.getString("name").getBytes(StandardCharsets.UTF_8), Charset.defaultCharset()),
                resultSet.getInt("salary")));
    }

    public static List<YoungestEldestWorkers> findYoungestEldestWorkers() {
        return selectQuery("sql/find_youngest_eldest_workers.sql", (resultSet) -> new YoungestEldestWorkers(
                new String(resultSet.getString("TYPE").getBytes(StandardCharsets.UTF_8), Charset.defaultCharset()),
                new String(resultSet.getString("name").getBytes(StandardCharsets.UTF_8), Charset.defaultCharset()),
                LocalDate.parse(resultSet.getString("birthday"))));
    }

    public static List<ProjectPrices> printProjectPrices() {
        return selectQuery("sql/print_project_prices.sql", (resultSet) -> new ProjectPrices(
                new String(resultSet.getString("name").getBytes(StandardCharsets.UTF_8), Charset.defaultCharset()),
                resultSet.getInt("price")));
    }
}
