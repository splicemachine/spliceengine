package com.splicemachine.derby.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.junit.Ignore;

import com.splicemachine.constants.SpliceConstants;

@Ignore
public class SpliceDerbyTest {
	private static final Logger LOG = Logger.getLogger(SpliceDerbyTest.class);
    protected static String framework = "embedded";
    protected static String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    protected static String protocol = "jdbc:derby:splice/";
    protected static Properties props = new Properties();
	protected static Connection conn = null;
	protected static List<Statement> statements = new ArrayList<Statement>();
	
    protected static void loadDriver() {
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

    public static void startConnection() throws Exception {
        loadDriver();
        conn = DriverManager.getConnection(protocol +  SpliceConstants.SPLICE_DB + ";create=true", props);
	}
	
	public static void stopConnection() throws SQLException {
	    //Connection
	    try {
	        if (conn != null) {
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                }
	            conn.close();
	            conn = null;
	        }
	    } catch (SQLException sqle) {
	        printSQLException(sqle);
	    }		
	}

    
	public static ResultSet executeQuery (String sql) {
        try {
        	Statement s = conn.createStatement();
        	statements.add(s);
        	return s.executeQuery(sql);
        } catch (SQLException sqle) {
        	sqle.printStackTrace();
            printSQLException(sqle);
        }
        return null;
}

	public static void executeStatement (String sql) throws SQLException {
		Statement s = null;
        try {
            s = conn.createStatement();
            statements.add(s);
            s.execute(sql);
        } catch (SQLException sqle) {
        	sqle.printStackTrace();
            printSQLException(sqle);
        } finally {
        	if (s!= null)
        		s.close();
        }
     }

	protected static void closeStatements () throws SQLException {
		for (Statement statement: statements) 
			statement.close();
     }

	protected static void dropTable(String tableName) throws SQLException {	
		Statement s = null;
		try {
			conn.setAutoCommit(true);
			s = conn.createStatement();
			s.execute("drop table "+tableName);
		} catch (SQLException e) {
			LOG.error("error on drop table-"+tableName+": "+e.getMessage(), e);
		} finally {
			try {
				if (s != null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}		
	}
}
