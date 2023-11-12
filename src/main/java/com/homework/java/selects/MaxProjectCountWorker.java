package com.homework.java.selects;

public class MaxProjectCountWorker implements ResPrint {
    private String name;
    private int projectCount;

    public MaxProjectCountWorker(String name, int projectCount) {
        this.name = name;
        this.projectCount = projectCount;
    }

    @Override
    public String toString() {
        return "MaxProjectCountWorker{" +
                "name='" + name + '\'' +
                ", projectCount=" + projectCount +
                '}';
    }

    public String getName() {
        return name;
    }

    public int getProjectCount() {
        return projectCount;
    }
}
