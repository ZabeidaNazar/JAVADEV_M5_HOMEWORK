package com.homework.java.selects;

import java.time.LocalDate;

public class YoungestEldestWorkers implements ResPrint {
    private AgeType type;
    private String name;
    private LocalDate birthday;

    public YoungestEldestWorkers(String type, String name, LocalDate birthday) {
        this.type = AgeType.valueOf(type);
        this.name = name;
        this.birthday = birthday;
    }

    private enum AgeType {
        OLDEST,
        YOUNGEST
    }

    @Override
    public String toString() {
        return "YoungestEldestWorkers{" +
                "type='" + type + '\'' +
                ", name='" + name + '\'' +
                ", birthday=" + birthday +
                '}';
    }
}
