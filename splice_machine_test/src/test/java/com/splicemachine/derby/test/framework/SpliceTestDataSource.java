package com.splicemachine.derby.test.framework;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;

/**
 * A partial DataSource implementation that allows pooling Connections by Database URL.
 */
public class SpliceTestDataSource implements DataSource {
    private static final Logger LOG = Logger.getLogger(SpliceTestDataSource.class);

    public static final String DB_URL_TEMPLATE = "jdbc:splice://%s:%s/" + SpliceConstants.SPLICE_DB + ";create=true;user=%s;password=%s";
    public static final String DEFAULT_HOST = "localhost";
    public static final String DEFAULT_PORT = SpliceConstants.DEFAULT_DERBY_BIND_PORT+ "";
    public static final String DEFAULT_USER = "splice";
    public static final String DEFAULT_USER_PASSWORD = "admin";

    private final Map<String, List<Connection>> userConnections = new HashMap<>();

    // ====================================================================================================
    // DataSource interface
    // ====================================================================================================

    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(createURLString(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_USER, DEFAULT_USER_PASSWORD));
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getConnection(createURLString(DEFAULT_HOST, DEFAULT_PORT, username, password));
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        // TODO: impl
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        // TODO: impl
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        // TODO: impl
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        // TODO: impl
        return 0;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        // TODO: impl
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        // TODO: impl
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // TODO: impl
        return false;
    }

    // ====================================================================================================
    // Splice test interface
    // ====================================================================================================

    /**
     * Get a connection to the JDBC interface at the given host and port using the default splice
     * user and password.
     * @param host host to connect to
     * @param port the JDBC port to connect to
     * @return a valid connection
     * @throws SQLException
     */
    public Connection getConnection(String host, int port) throws SQLException {
        return getConnection(createURLString(host, port+"", DEFAULT_USER, DEFAULT_USER_PASSWORD));
    }

    public void shutdown() {
        for (List<Connection> connections : this.userConnections.values()) {
            for (Connection connection : connections) {
                try {
                    if (connection != null && ! connection.isClosed()) {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            connection.rollback();
                            connection.close();
                        }
                    }
                } catch (SQLException e) {
                    LOG.warn("Unable to close connection! Ignoring.");
                }
            }
        }
        this.userConnections.clear();
    }

    public List<String> connectionStatus() {
        List<String> connectionStrs = new ArrayList<>();
        for (Map.Entry<String,List<Connection>> entry : this.userConnections.entrySet()) {
            int open = 0;
            int closed = 0;
            int problem = 0;
            for (Connection connection : entry.getValue()) {
                try {
                    if (connection.isClosed()) {
                        ++closed;
                    } else {
                        ++open;
                    }
                } catch (SQLException e) {
                    ++problem;
                }
            }
            connectionStrs.add(entry.getKey() + " Open: "+ open + " Closed: "+closed+ " Problem: "+problem);
        }
        return connectionStrs;
    }

    // ====================================================================================================
    // Helpers
    // ====================================================================================================

    private String createURLString(String host, String port, String userName, String password) {
        return String.format(DB_URL_TEMPLATE, host, port, userName, password);
    }

    /**
     * Get the connections (currently only one) tied to a DB URL (including host, port, user and pwd)
     * @param url the full connection URL
     * @return (currently the only) connection already tied to a given URL or a new one
     * @throws SQLException
     */
    private Connection getConnection(String url) throws SQLException {
        if (this.userConnections.size() == 0) {
            List<Connection> connections = new ArrayList<>();
            connections.add(DriverManager.getConnection(url, new Properties()));
            this.userConnections.put(url.toUpperCase(), connections);
            return connections.get(0);
        } else {
            List<Connection> connections = this.userConnections.get(url);
            if (connections == null) {
                connections = new ArrayList<>();
                connections.add(DriverManager.getConnection(url, new Properties()));
                this.userConnections.put(url.toUpperCase(), connections);
            }
            return connections.get(0);
        }
    }
}
