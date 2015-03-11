package com.splicemachine.derby.test.framework;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.Ignore;

import com.splicemachine.constants.SpliceConstants;

@Ignore
public class SpliceEmbedConnection {
    protected static String framework = "embedded";
    protected static String driver = "com.splicemachine.db.jdbc.EmbeddedDriver";
    protected static String protocol = "jdbc:derby:splice/";
    protected static Properties props = new Properties();
	protected static Connection conn = null;
	protected static List<Statement> statements = new ArrayList<Statement>();
	protected static boolean loaded;
	
    protected synchronized static void loadDriver() {
        try {
            Class.forName(driver).newInstance();
        } catch (ClassNotFoundException cnfe) {
            System.err.println("\nUnable to load the JDBC driver " + driver);
            System.err.println("Please check your CLASSPATH.");
            cnfe.printStackTrace(System.err);
        } catch (InstantiationException ie) {
            System.err.println(
                        "\nUnable to instantiate the JDBC driver " + driver);
            ie.printStackTrace(System.err);
        } catch (IllegalAccessException iae) {
            System.err.println(
                        "\nNot allowed to access the JDBC driver " + driver);
            iae.printStackTrace(System.err);
        }
    }
    
    protected static void printSQLException(SQLException e) {
        while (e != null)
        {
            System.err.println("\n----- SQLException -----");
            System.err.println("  SQL State:  " + e.getSQLState());
            System.err.println("  Error Code: " + e.getErrorCode());
            System.err.println("  Message:    " + e.getMessage());
            e = e.getNextException();
        }
    }

    
    public static Connection getConnection() throws Exception {
    	if (!loaded)
    		loadDriver();
        return DriverManager.getConnection(protocol + SpliceConstants.SPLICE_DB + ";create=true", props);
    }
    
}
