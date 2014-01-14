package com.splicemachine.test.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import org.apache.log4j.Logger;

/**
 * Static helper class to get an embedded connection to Derby
 */
public class DerbyEmbedConnection extends BaseConnection {
    private static final Logger LOG = Logger.getLogger(DerbyEmbedConnection.class);

    protected static String protocol = "jdbc:derby:";
    protected static String db = "derbyDB";

    private static String createProtocol(String port, boolean createDB) {
        // port ignored for embedded connection
        return protocol+db+(createDB?create:"");
    }

    private static synchronized Connection createConnection(String port) throws Exception {
        loadDriver(embeddedDriver);
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
