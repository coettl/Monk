package com.linemetrics.monk.config.sqlite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public abstract class SqliteConnector {

    private String      dbFile = null;
    private Connection  c = null;

    protected SqliteConnector(String dbFile) {
        this.dbFile = dbFile;
    }

    protected void connectDB() throws SqliteException {
        try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:" + dbFile);
            c.setAutoCommit(true);
        } catch(ClassNotFoundException | SQLException exp) {
            throw new SqliteException(exp.getMessage());
        }
    }

    protected void closeDB() throws SqliteException {
        try {
            c.close();
        } catch(SQLException exp) {
            throw new SqliteException(exp.getMessage());
        }
    }

    protected Connection getConnection() {
        return this.c;
    }

}
