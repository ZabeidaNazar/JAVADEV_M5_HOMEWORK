package com.homework.java;

import com.google.gson.*;
import com.homework.java.db.Database;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DatabasePopulateService {
    public static String insertToWorker = "INSERT INTO worker (name, birthday, level, salary) VALUES (?, ?, ?, ?)";
    public static String insertToClient = "INSERT INTO client (name) VALUES (?)";
    public static String insertToProject = "INSERT INTO project (client_id, name, start_date, finish_date) VALUES (?, ?, ?, ?)";
    public static String insertToProjectWorker = "INSERT INTO project_worker (project_id, worker_id) VALUES (?, ?)";

    public static void main(String[] args) {
        Database database = Database.getInstance();
        Connection connection = database.getConnection();

        List<Worker> workers = readJsonData("data/worker.json", Worker.class);
        List<Client> clients = readJsonData("data/client.json", Client.class);
        List<Project> projects = readJsonData("data/project.json", Project.class);
        List<ProjectWorker> projectWorkers = readJsonData("data/project_worker.json", ProjectWorker.class);

        executePreparedStatement(insertToWorker, workers, 60, (preparedStatement, worker) -> {
            preparedStatement.setString(1, worker.getName());
            preparedStatement.setDate(2, Date.valueOf(worker.getBirthday()));
            preparedStatement.setString(3, worker.getLevel().name());
            preparedStatement.setInt(4, worker.getSalary());
        });

        executePreparedStatement(insertToClient, clients, 50, (preparedStatement, client) ->
                preparedStatement.setString(1, client.getName()));

        executePreparedStatement(insertToProject, projects, 40, (preparedStatement, project) -> {
            preparedStatement.setInt(1, project.getClientId());
            preparedStatement.setString(2, project.getName());
            preparedStatement.setDate(3, Date.valueOf(project.getStartDate()));
            preparedStatement.setDate(4, Date.valueOf(project.getFinishDate()));
        });

        executePreparedStatement(insertToProjectWorker, projectWorkers, 100, (preparedStatement, projectWorker) -> {
            preparedStatement.setInt(1, projectWorker.getProjectId());
            preparedStatement.setInt(2, projectWorker.getWorkerId());
        });

        try {
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException("SQLException", e);
        }
    }

    public static <T> int executePreparedStatement(String query, List<T> elements, int countInBatch,
                                                   SQLBiConsumer<PreparedStatement, T> fillPreparedStatement) {
        Database database = Database.getInstance();
        Connection connection = database.getConnection();

        int elementsCount = elements.size();
        int actualCount = 0;

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);

            for (int i = 1; i <= elementsCount; i++) {
                fillPreparedStatement.accept(preparedStatement, elements.get(i - 1));
                preparedStatement.addBatch();
                if (i % countInBatch == 0 || i == elements.size()) {
                    actualCount += Arrays.stream(preparedStatement.executeBatch()).sum();
                }
            }

            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException("SQLException.", e);
        } finally {
            System.out.println("Updated rows: " + actualCount + "/" + elementsCount + " - " + query.split("VALUES")[0]);
        }

        return actualCount;
    }

    public static <T> List<T> readJsonData(String pathToFile, Class<T> tClass) {
        List<T> list = new ArrayList<>();

        Gson gson = new GsonBuilder().registerTypeAdapter(LocalDate.class,
                (JsonDeserializer<LocalDate>) (json, type, jsonDeserializationContext) -> {
            return LocalDate.parse(json.getAsJsonPrimitive().getAsString(), DateTimeFormatter.ISO_LOCAL_DATE); // yyyy-mm-dd
        }).create();
        try(FileReader reader = new FileReader(pathToFile, StandardCharsets.UTF_8)) {
            JsonArray jsonArray = JsonParser.parseReader(reader).getAsJsonArray();
            for (JsonElement jsonElement : jsonArray) {
                list.add(gson.fromJson(jsonElement, tClass));
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not fount in " + pathToFile, e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return list;
    }

    static class Worker {
        private String name;
        private LocalDate birthday;
        private Level level;
        private int salary;

        public Worker(String name, LocalDate birthday, Level level, int salary) {
            this.name = name;
            this.birthday = birthday;
            this.level = level;
            this.salary = salary;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public LocalDate getBirthday() {
            return birthday;
        }

        public void setBirthday(LocalDate birthday) {
            this.birthday = birthday;
        }

        public Level getLevel() {
            return level;
        }

        public void setLevel(Level level) {
            this.level = level;
        }

        public int getSalary() {
            return salary;
        }

        public void setSalary(int salary) {
            this.salary = salary;
        }

        public enum Level {
            Trainee,
            Junior,
            Middle,
            Senior
        }
    }
    static class Client {
        private String name;

        public Client(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
    static class Project {
        private int client_id;
        private String name;
        private LocalDate start_date;
        private LocalDate finish_date;

        public Project(int clientId, String name, LocalDate startDate, LocalDate finishDate) {
            this.client_id = clientId;
            this.name = name;
            this.start_date = startDate;
            this.finish_date = finishDate;
        }

        public int getClientId() {
            return client_id;
        }

        public void setClientId(int clientId) {
            this.client_id = clientId;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public LocalDate getStartDate() {
            return start_date;
        }

        public void setStartDate(LocalDate startDate) {
            this.start_date = startDate;
        }

        public LocalDate getFinishDate() {
            return finish_date;
        }

        public void setFinishDate(LocalDate finishDate) {
            this.finish_date = finishDate;
        }
    }
    static class ProjectWorker {
        private int project_id;
        private int worker_id;

        public ProjectWorker(int projectId, int workerId) {
            this.project_id = projectId;
            this.worker_id = workerId;
        }

        public int getProjectId() {
            return project_id;
        }

        public void setProjectId(int projectId) {
            this.project_id = projectId;
        }

        public int getWorkerId() {
            return worker_id;
        }

        public void setWorkerId(int workerId) {
            this.worker_id = workerId;
        }
    }
}
