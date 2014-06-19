package com.splicemachine.derby.test.framework;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.test.SpliceNetDerbyTest;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Convenient factory for obtaining java.sql.Connection to LOCAL splice from tests.
 */
public class SpliceNetConnection {

    private static final Logger LOG = Logger.getLogger(SpliceNetDerbyTest.class);

    private static final String DRIVER_CLASS = "org.apache.derby.jdbc.ClientDriver";
    private static final String DB_URL_LOCAL = "jdbc:derby://localhost:1527/";

    private static boolean driverClassLoaded;

    public static Connection getConnection() {
    	return getConnectionAs("splice", "admin");
    }

    public static Connection getConnectionAs(String userName, String password) {
        if (!driverClassLoaded) {
            loadDriver();
        }
        Properties props = new Properties();
        try {
        	return DriverManager.getConnection(String.format("%s%s;create=true;user=%s;password=%s",DB_URL_LOCAL,SpliceConstants.SPLICE_DB,userName, password), props);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private synchronized static void loadDriver() {
        SpliceLogUtils.trace(LOG, "Loading the JDBC Driver");
        try {
            Class.forName(DRIVER_CLASS).newInstance();
            driverClassLoaded = true;
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Unable to load the JDBC driver " + DRIVER_CLASS + " Please check your CLASSPATH.");
        } catch (InstantiationException e) {
            throw new IllegalStateException("Unable to instantiate the JDBC driver " + DRIVER_CLASS);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Not allowed to access the JDBC driver " + DRIVER_CLASS);
        }
    }

}
