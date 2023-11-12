package com.homework.java.selects;

public class MaxProjectCountClient implements ResPrint {
    private String name;
    private int projectCount;

    public MaxProjectCountClient(String name, int projectCount) {
        this.name = name;
        this.projectCount = projectCount;
    }

    @Override
    public String toString() {
        return "MaxProjectCountClient{" +
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
