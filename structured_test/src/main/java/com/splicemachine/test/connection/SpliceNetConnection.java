package com.splicemachine.test.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

/**
 * Static helper class to get an embedded connection to Splice
 */
public class SpliceNetConnection {
	private static final Logger LOG = Logger.getLogger(SpliceNetConnection.class);

    protected static String driver = "org.apache.derby.jdbc.ClientDriver";
    protected static String protocol = "jdbc:derby://localhost:1527/spliceDB;create=true";
    protected static String protocol2 = "jdbc:derby://localhost:1527/spliceDB";
    protected static Properties props = new Properties();
	protected static boolean loaded;

    protected static synchronized void loadDriver() throws Exception{
        SpliceLogUtils.trace(LOG, "Loading the JDBC Driver");
        try {
            Class.forName(driver).newInstance();
        } catch (ClassNotFoundException e) {
            String msg = "\nUnable to load the JDBC driver " + driver + " Please check your CLASSPATH.";
            LOG.error(msg, e);
            throw e;
        } catch (InstantiationException e) {
            String msg = "\nUnable to instantiate the JDBC driver " + driver;
            LOG.error(msg, e);
            throw e;
        } catch (IllegalAccessException e) {
            String msg = "\nNot allowed to access the JDBC driver " + driver;
            LOG.error(msg, e);
            throw e;
        }
        loaded =  true;
    }

    private static synchronized Connection createConnection() throws Exception {
        loadDriver();
        return DriverManager.getConnection(protocol, props);
    }

    /**
     * Acquire a connection
     * @return a new connection
     * @throws Exception for any failure
     */
    public static Connection getConnection() throws Exception {
        if (!loaded) {
            return createConnection();
        } else {
            return DriverManager.getConnection(protocol2, props);
        }
    }
}
