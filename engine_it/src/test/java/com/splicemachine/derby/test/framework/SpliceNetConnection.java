package com.splicemachine.derby.test.framework;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Convenient factory for obtaining java.sql.Connection to LOCAL splice from tests.
 */
public class SpliceNetConnection {

    private static final String DB_URL_LOCAL = "jdbc:splice://localhost:1527/splicedb;create=true;user=%s;password=%s";
    public static final String DEFAULT_USER = "splice";
    public static final String DEFAULT_USER_PASSWORD = "admin";

    public static Connection getConnection() throws SQLException {
        return getConnectionAs(DEFAULT_USER, DEFAULT_USER_PASSWORD);
    }

    public static Connection getConnectionAs(String userName, String password) throws SQLException {
        return getConnectionAs(DB_URL_LOCAL, userName, password);
    }

    public static String getDefaultLocalURL() {
        return String.format(DB_URL_LOCAL, DEFAULT_USER, DEFAULT_USER_PASSWORD);
    }

    private static Connection getConnectionAs(String providedURL, String userName, String password) throws SQLException {
        String url = String.format(providedURL, userName, password);
        return DriverManager.getConnection(url, new Properties());
    }

}