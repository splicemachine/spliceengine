package com.splicemachine.derby.test.framework;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.jdbc.ClientDriver;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Convenient factory for obtaining java.sql.Connection to LOCAL splice from tests.
 */
public class SpliceNetConnection {

    private static final Logger LOG = Logger.getLogger(SpliceNetConnection.class);

    private static final String DB_URL_LOCAL = "jdbc:derby://localhost:1527/" + SpliceConstants.SPLICE_DB + ";create=true;user=%s;password=%s";
    public static final String DEFAULT_USER = "splice";
    public static final String DEFAULT_USER_PASSWORD = "admin";

    private static boolean driverClassLoaded;

    public static Connection getConnection() throws SQLException {
        return getConnectionAs(DEFAULT_USER, DEFAULT_USER_PASSWORD);
    }

    public static Connection getConnectionAs(String userName, String password) throws SQLException {
        String url = String.format(DB_URL_LOCAL, userName, password);
        loadDriver();
        Connection connection = DriverManager.getConnection(url, new Properties());
        return connection;
    }

    private synchronized static void loadDriver() {
        if(!driverClassLoaded) {
            SpliceLogUtils.trace(LOG, "Loading the JDBC Driver");
            try {
                DriverManager.registerDriver(new ClientDriver());
                driverClassLoaded = true;
            } catch (SQLException e) {
                throw new IllegalStateException("Unable to load the JDBC driver.");
            }
        }
    }

}