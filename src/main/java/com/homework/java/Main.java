package com.homework.java;

import com.homework.java.db.DB;
import com.homework.java.db.Database;
import com.homework.java.selects.*;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        Database.getInstance().setDefaultDB(DB.POSTGRES);
        v2();
    }

    public static void v1() {

        List<MaxProjectCountClient> maxProjectsClient = DatabaseQueryService.findMaxProjectsClient();
        for (MaxProjectCountClient item : maxProjectsClient) {
            System.out.println(item);
        }

        List<MaxProjectCountWorker> maxProjectsWorker = DatabaseQueryService.findMaxProjectsWorker();
        for (MaxProjectCountWorker item : maxProjectsWorker) {
            System.out.println(item);
        }

        List<LongestProject> longestProject = DatabaseQueryService.findLongestProject();
        for (LongestProject item : longestProject) {
            System.out.println(item);
        }

        List<MaxSalaryWorker> maxSalaryWorker = DatabaseQueryService.findMaxSalaryWorker();
        for (MaxSalaryWorker item : maxSalaryWorker) {
            System.out.println(item);
        }

        List<YoungestEldestWorkers> youngestEldestWorkers = DatabaseQueryService.findYoungestEldestWorkers();
        for (YoungestEldestWorkers item : youngestEldestWorkers) {
            System.out.println(item);
        }

        List<ProjectPrices> projectPrices = DatabaseQueryService.printProjectPrices();
        for (int i = 1; i <= projectPrices.size(); i++) {
            System.out.print(i);
            System.out.print(" - ");
            System.out.println(projectPrices.get(i - 1));
        }
    }

    public static void v2() {
        List<List<? extends ResPrint>> selectsResults = new ArrayList<>();

        selectsResults.add(DatabaseQueryService.findMaxProjectsClient());
        selectsResults.add(DatabaseQueryService.findMaxProjectsWorker());
        selectsResults.add(DatabaseQueryService.findLongestProject());
        selectsResults.add(DatabaseQueryService.findMaxSalaryWorker());
        selectsResults.add(DatabaseQueryService.findYoungestEldestWorkers());
        selectsResults.add(DatabaseQueryService.printProjectPrices());

        int i;
        for (List<? extends ResPrint> selectsResult : selectsResults) {
            for (i = 1; i <= selectsResult.size(); i++) {
                System.out.print(i);
                System.out.print(" - ");
                System.out.println(selectsResult.get(i - 1));
            }
            System.out.println();
        }

        Database.close();
    }
}