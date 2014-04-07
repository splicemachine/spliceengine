package com.splicemachine.test.connection;

import com.splicemachine.constants.SpliceConstants;
import java.sql.Connection;
import java.sql.DriverManager;
import org.apache.log4j.Logger;

/**
 * Static helper class to get an client connection to Splice
 */
public class SpliceNetConnection extends BaseConnection {
	private static final Logger LOG = Logger.getLogger(SpliceNetConnection.class);

    private static String driver = "org.apache.derby.jdbc.ClientDriver";
    private static String protocol = "jdbc:derby://localhost:";
    private static String db = "/splicedb";

    private static String createProtocol(String port, boolean createDB) {
        return protocol+(port != null?port: SpliceConstants.DEFAULT_DERBY_BIND_PORT)+db+(createDB?create:"");
    }

    private static synchronized Connection createConnection(String port) throws Exception {
        loadDriver(clientDriver);
        return DriverManager.getConnection(createProtocol(port, true), props);
    }

    /**
     * Acquire a connection
     * @param port optional port to connect to
     * @return a new connection
     * @throws Exception for any failure
     */
    public static Connection getConnection(String port) throws Exception {
        if (!loaded) {
            return createConnection(port);
        } else {
            return DriverManager.getConnection(createProtocol(port,false), props);
        }
    }

}
