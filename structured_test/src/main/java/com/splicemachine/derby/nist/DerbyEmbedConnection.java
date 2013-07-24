package com.splicemachine.derby.nist;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.Ignore;

@Ignore
public class DerbyEmbedConnection implements ConnectionFactory {
    protected static String framework = "embedded";
    protected static String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    protected static String protocol = "jdbc:derby:derbyDB;create=true";
    protected static String protocol2 = "jdbc:derby:derbyDB";
    protected static Properties props = new Properties();
	protected static Connection conn = null;
	protected static List<Statement> statements = new ArrayList<Statement>();
	protected static boolean loaded;

    public DerbyEmbedConnection() throws Exception {
        loadDriver();
        DriverManager.getConnection(protocol, props);
    }
	
    public synchronized void loadDriver() throws Exception {
        try {
            Class.forName(driver).newInstance();
        } catch (ClassNotFoundException cnfe) {
            System.err.println("\nUnable to load the JDBC driver " + driver);
            System.err.println("Please check your CLASSPATH.");
            cnfe.printStackTrace(System.err);
            throw cnfe;
        } catch (InstantiationException ie) {
            System.err.println(
                        "\nUnable to instantiate the JDBC driver " + driver);
            ie.printStackTrace(System.err);
            throw ie;
        } catch (IllegalAccessException iae) {
            System.err.println(
                        "\nNot allowed to access the JDBC driver " + driver);
            iae.printStackTrace(System.err);
            throw iae;
        }
        loaded =  true;
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

    
//    private Connection createConnection() throws Exception {
//        return DriverManager.getConnection(protocol, props);
//    }

    public Connection getConnection() throws Exception {
//    	if (!loaded) {
//    		loadDriver();
//        }
        return DriverManager.getConnection(protocol, props);
    }

}
