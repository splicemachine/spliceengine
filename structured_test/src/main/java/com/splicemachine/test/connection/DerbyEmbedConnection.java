package com.splicemachine.test.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

/**
 * Static helper class to get an embedded connection to Derby
 */
public class DerbyEmbedConnection {
    private static final Logger LOG = Logger.getLogger(DerbyEmbedConnection.class);

    protected static String framework = "embedded";
	protected static Connection conn = null;
	protected static List<Statement> statements = new ArrayList<Statement>();

    protected static String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    protected static String protocol = "jdbc:derby:derbyDB;create=true";
    protected static String protocol2 = "jdbc:derby:derbyDB";
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
