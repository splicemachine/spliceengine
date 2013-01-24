package com.splicemachine.derby.hbase;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.types.J2SEDataValueFactory;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.services.reflect.ReflectClassesJava2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
/**
 * 
 * Singleton class for handling derby between the hbase coprocessors that need to access it.
 * 
 * @author johnleach
 *
 */
public class SpliceEngine {
	private static Logger LOG = Logger.getLogger(SpliceEngine.class);
	public static byte[] TEMP_TABLE = "SYS_TEMP".getBytes();
	protected static J2SEDataValueFactory dvf = new J2SEDataValueFactory();
	protected static ReflectClassesJava2 classLoader = new ReflectClassesJava2();
	protected static ModuleFactory monitor;
    protected static String framework = "embedded";
    protected static String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    protected static String protocol = "jdbc:derby:splice:";
    protected static Properties props = new Properties();
	protected static final String dbName = "wombat";
	protected static EmbedConnection conn;
	protected static LanguageConnectionContext lcc;
	protected static boolean init = false;

	/**
	 * Initialize method that tests for the temp table and creates it if needed...
	 * 
	 */
	public static synchronized void init() {
		if (init == true) return;
		SpliceLogUtils.trace(LOG, "init called for Splice Engine");
		try {
			startConnection();
		} catch (SQLException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, e);
		}
		try {
			HBaseAdmin admin = new HBaseAdmin(new Configuration());
			if (!admin.tableExists(TEMP_TABLE)) {
				HTableDescriptor td = SpliceUtils.generateDefaultDescriptor(Bytes.toString(TEMP_TABLE));
				admin.createTable(td);
				SpliceLogUtils.info(LOG, TEMP_TABLE + " created");
			}
			admin.close();
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, e);
		} 
		init = true;
	}
		/**
		 * 
		 * Loading the Derby Driver
		 * 
		 */
	   protected static void loadDriver() {
		   Monitor.clearMonitor();
	        try {
	    		SpliceLogUtils.trace(LOG, "Attempting to Load the appropriate driver");
	        	Class.forName(driver).newInstance();
	        } catch (ClassNotFoundException cnfe) {
	        	SpliceLogUtils.logAndThrowRuntime(LOG, "Class Not Found Exception " + cnfe.getMessage(), cnfe);
	        } catch (InstantiationException ie) {
	        	SpliceLogUtils.logAndThrowRuntime(LOG, "InstantiationException", ie);
	        } catch (IllegalAccessException iae) {
	        	SpliceLogUtils.logAndThrowRuntime(LOG, "IllegalAccessException", iae);
	        }
	    }
		/**
		 * 
		 * Prints the SQLEsxception that is thrown.
		 * 
		 */
	    protected static void printSQLException(SQLException e) {
	        while (e != null) {
	            System.err.println("\n----- SQLException -----");
	            System.err.println("  SQL State:  " + e.getSQLState());
	            System.err.println("  Error Code: " + e.getErrorCode());
	            System.err.println("  Message:    " + e.getMessage());
	            e = e.getNextException();
	        }
	    }	    
	    /**
	     * Starts the database connection and grabs the language connection context.
	     * 
	     * @throws SQLException
	     */
		public static void startConnection() throws SQLException {
    		SpliceLogUtils.trace(LOG, "startConnection");
			loadDriver();
			try {
				conn = (EmbedConnection) DriverManager.getConnection(protocol + dbName + ";create=true", props);
				lcc = conn.getLanguageConnection();
			}
			catch (Exception e) {
				SpliceLogUtils.logAndThrow(LOG, "Cannot Start Connection " + e.getMessage() + ", conn: " + conn, new SQLException(e));
			}
		}		
		
		/**
		 * Stops the derby connection.
		 * 
		 * @throws SQLException
		 */
		public static void stopConnection() throws SQLException {
    		SpliceLogUtils.trace(LOG, "stopConnection");
		    try {
		        if (conn != null) {
		            conn.close();
		            conn = null;
		        }
		    } catch (SQLException sqle) {
		        printSQLException(sqle);
		    }		
		}
		/**
		 * Retrieves the language Connection Context from the derby connection
		 * 
		 * @return
		 */
		public static LanguageConnectionContext getLanguageConnectionContext() {
			if (!init)
				init();
			return lcc;
		}
}
