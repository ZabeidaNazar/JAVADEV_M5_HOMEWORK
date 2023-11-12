package com.homework.java;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class SqlParser {

    public static List<String> getSqlCommands(String sqlFile, int maxQueryCount) {
        File file = new File(sqlFile);

        Scanner scanner;
        try {
            scanner = new Scanner(file, StandardCharsets.UTF_8);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("За вказаним шляхом файлу не існує!");
        } catch (IOException e) {
            throw new RuntimeException("IOException. Reason: " + e.getMessage());
        }

        List<String> queryPacks = new ArrayList<>();
        int queryCount = 0;
        StringBuilder queryPack = new StringBuilder();
        String nextLine;
        while (true) {
            if (!scanner.hasNextLine()) {
                if (!queryPack.isEmpty()) {
                    queryPacks.add(queryPack.toString());
                    queryPack.setLength(0);
                    queryPack.trimToSize();
                }
                break;
            }

            nextLine = scanner.nextLine();
            if (nextLine.isEmpty() || nextLine.startsWith("--")) {
                continue;
            }
            queryPack.append(nextLine).append("\n");
            if (nextLine.charAt(nextLine.length() - 1) == ';' && ++queryCount == maxQueryCount) {
                queryPacks.add(queryPack.toString());

                queryCount = 0;

                queryPack.setLength(0);
                queryPack.trimToSize();
            }
        }

        scanner.close();
        return queryPacks;
    }
}
