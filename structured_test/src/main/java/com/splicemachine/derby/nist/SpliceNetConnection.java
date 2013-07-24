package com.splicemachine.derby.nist;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;

public class SpliceNetConnection implements ConnectionFactory {
	private static final Logger LOG = Logger.getLogger(SpliceNetConnection.class);
    protected static String framework = "client";
    protected static String driver = "org.apache.derby.jdbc.ClientDriver";
    protected static String protocol = "jdbc:derby://localhost:1527/";
    protected static Properties props = new Properties();
	protected static Connection conn = null;
	protected static List<Statement> statements = new ArrayList<Statement>();
	protected static boolean loaded;

    public SpliceNetConnection() throws Exception {
        loadDriver();
    }
		
    public synchronized void loadDriver() throws Exception{
    	SpliceLogUtils.trace(LOG, "Loading the JDBC Driver");
        try {
            Class.forName(driver).newInstance();
        } catch (ClassNotFoundException e) {
            System.err.println("\nUnable to load the JDBC driver " + driver);
            System.err.println("Please check your CLASSPATH.");
            e.printStackTrace(System.err);
            throw e;
        } catch (InstantiationException e) {
            System.err.println(
                        "\nUnable to instantiate the JDBC driver " + driver);
            e.printStackTrace(System.err);
            throw e;
        } catch (IllegalAccessException e) {
            System.err.println(
                        "\nNot allowed to access the JDBC driver " + driver);
            e.printStackTrace(System.err);
            throw e;
        }
        loaded =  true;
    }

    public Connection getConnection() throws Exception {
//        if (!loaded)
//            loadDriver();
        return DriverManager.getConnection(protocol + "spliceDB;create=true", props);
    }
}
