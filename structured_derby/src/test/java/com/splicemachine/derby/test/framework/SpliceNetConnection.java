package com.splicemachine.derby.test.framework;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.dbutils.DbUtils;
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

    private static final String DB_URL_LOCAL = "jdbc:derby://localhost:1527/";
    public static final String DEFAULT_USER = "splice";
    public static final String DEFAULT_USER_PASSWORD = "admin";

    private static boolean storedStatementsCompiled;
    private static boolean driverClassLoaded;

    public static Connection getConnection() {
        return getConnectionAs(DEFAULT_USER, DEFAULT_USER_PASSWORD);
    }

    public static Connection getConnectionAs(String userName, String password) {
        if (!driverClassLoaded) {
            loadDriver();
        }
        compileAllInvalidStoredStatements();
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
            DriverManager.registerDriver(new ClientDriver());
            driverClassLoaded = true;
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to load the JDBC driver.");
        }
    }

    /**
     * Temporary workaround for bug DB-1342 to allow ITs to run concurrently. Call stored procedure to compile all
     * system stored statements  Do this once per IT JVM.  See bug DB-1342 for details.  This method/call can be removed
     * when we fix 1342.
     */
    public static synchronized void compileAllInvalidStoredStatements() {
        if (!storedStatementsCompiled) {
            long startTime = System.currentTimeMillis();
            String SQL = "call SYSCS_UTIL.SYSCS_RECOMPILE_INVALID_STORED_STATEMENTS()";
            Connection connection = null;
            try {
                connection = DriverManager.getConnection(DB_URL_LOCAL + SpliceConstants.SPLICE_DB + ";create=true", new Properties());
                connection.prepareCall(SQL).execute();
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            } finally {
                DbUtils.commitAndCloseQuietly(connection);
            }
            storedStatementsCompiled = true;
            LOG.info(SQL + " time (ms) = " + (System.currentTimeMillis() - startTime));
        }
    }

}
