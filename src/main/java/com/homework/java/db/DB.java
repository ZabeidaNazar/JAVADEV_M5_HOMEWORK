package com.homework.java.db;

import com.homework.java.db.creators.Creator;
import com.homework.java.db.creators.H2Creator;
import com.homework.java.db.creators.MysqlCreator;
import com.homework.java.db.creators.PostgresCreator;

import java.sql.Connection;

public enum DB {
    POSTGRES(new PostgresCreator()),
    H2(new H2Creator()),
    MYSQL(new MysqlCreator());

    private Creator creator;
    DB(Creator creator) {
        this.creator = creator;
    }

    public Connection getConnection() {
        return creator.createConnection();
    }
}
