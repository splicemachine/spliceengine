package com.splicemachine.derby.utils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.splicemachine.constants.SpliceConstants;

import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.jdbc.InternalDriver;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.io.Closeables;
import com.splicemachine.hbase.jmx.JMXUtils;
import com.splicemachine.pipeline.exception.Exceptions;

/**
 * Common static utility functions for subclasses which provide
 * implementation logic for utility system procedures,
 * which are associated with public static Java methods.
 * For example, many of our administrative system procedures
 * fetch their information from JMX beans, and common methods
 * are provided here to do so.
 * 
 * @see SpliceAdmin
 * @see TimestampAdmin
 */
public abstract class BaseAdminProcedures {

	// WARNING: do not add just any old logic here! This class is only intended
	// for static methods needed to support the back end system procedures.
	// Just because 'admin' is in the name does not mean anything you deem to be
	// related to system admin belongs here. We don't want this to be another
	// dumping around for utility functions.
	
    protected static List<String> getServerNames(Collection<ServerName> serverInfo) {
        List<String> names = new ArrayList<String>(serverInfo.size());
        for (ServerName sname : serverInfo) {
            names.add(sname.getHostname());
        }
        return names;
    }

	private static void throwNullArgError(Object value) {
	    throw new IllegalArgumentException(String.format("Required argument %s is null.", value));	
	}
	
    protected static interface JMXServerOperation {
        void operate(List<Pair<String, JMXConnector>> jmxConnector) throws MalformedObjectNameException, IOException, SQLException;
    }

    protected static void operate(JMXServerOperation operation, List<ServerName> serverNames) throws SQLException {
    	if (operation == null) throwNullArgError("operation");
        List<Pair<String, JMXConnector>> connections = null;
        try {
            connections = JMXUtils.getMBeanServerConnections(getServerNames(serverNames));
            operation.operate(connections);
        } catch (MalformedObjectNameException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        } catch (IOException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        } finally {
            if (connections != null) {
                for (Pair<String, JMXConnector> connectorPair : connections) {
                    Closeables.closeQuietly(connectorPair.getSecond());
                }
            }
        }
    }

    protected static void operate(JMXServerOperation operation) throws SQLException {
    	if (operation == null) throwNullArgError("operation");
        List<ServerName> regionServers = SpliceUtils.getServers();
        operate(operation, regionServers);
    }

    protected static void operateOnMaster(JMXServerOperation operation) throws SQLException {
        if (operation == null) throwNullArgError("operation");

        ServerName masterServer = SpliceUtils.getMasterServer();

        String serverName = masterServer.getHostname();
        String port = SpliceConstants.config.get("hbase.master.jmx.port","10101");
        try {
            JMXServiceURL url = new JMXServiceURL(String.format("service:jmx:rmi://%1$s/jndi/rmi://%1$s:%2$s/jmxrmi",serverName,port));
            JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
            operation.operate(Arrays.asList(Pair.newPair(serverName,jmxc)));
        } catch (IOException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        } catch (MalformedObjectNameException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
    }

    public static Connection getDefaultConn() throws SQLException {
        InternalDriver id = InternalDriver.activeDriver();
        if (id != null) {
            Connection conn = id.connect("jdbc:default:connection", null);
            if (conn != null)
                return conn;
        }
        throw Util.noCurrentConnection();
    }

    public static ResultSet executeStatement(StringBuilder sb) throws SQLException {
        ResultSet result = null;
        Connection connection = getDefaultConn();
        try {
            PreparedStatement ps = connection.prepareStatement(sb.toString());
            result = ps.executeQuery();
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw new SQLException(sb.toString(), e);
        } finally {
            connection.close();
        }
        return result;
    }

}
