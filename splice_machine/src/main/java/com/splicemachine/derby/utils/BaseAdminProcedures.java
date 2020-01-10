/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.utils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import javax.management.MalformedObjectNameException;
import javax.management.remote.JMXConnector;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.jdbc.Util;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.jdbc.InternalDriver;


import com.splicemachine.hbase.jmx.JMXUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.PartitionServer;
import com.splicemachine.utils.Pair;

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

    protected static List<Pair<String,String>> getServerNames(Collection<PartitionServer> serverInfo) {
        List<Pair<String,String>> names =new ArrayList<>(serverInfo.size());
        for (PartitionServer sname : serverInfo) {
            names.add(Pair.newPair(sname.getHostname(),sname.getHostAndPort()));
        }
        return names;
    }

	protected static void throwNullArgError(Object value) {
	    throw new IllegalArgumentException(String.format("Required argument %s is null.", value));	
	}
	
    protected interface JMXServerOperation {
        void operate(List<Pair<String, JMXConnector>> jmxConnector) throws MalformedObjectNameException, IOException, SQLException;
    }

    /**
     * Get the JMX connections for the region servers.
     *
     * @param serverNames
     * @return
     * @throws IOException
     */
    protected static List<Pair<String, JMXConnector>> getConnections(Collection<PartitionServer> serverNames) throws IOException {
    	return JMXUtils.getMBeanServerConnections(getServerNames(serverNames));
    }

    /**
     * Execute (or "operate") the JMX operation on the region servers.
     * JMX connections will be created and closed for each operation on each region server.
     *
     * @param operation
     * @param serverNames
     *
     * @throws SQLException
     */
    protected static void operate(JMXServerOperation operation, Collection<PartitionServer> serverNames) throws SQLException {
    	if (operation == null) throwNullArgError("operation");
        List<Pair<String, JMXConnector>> connections = null;
        try {
            connections = getConnections(serverNames);
            operation.operate(connections);
        } catch (MalformedObjectNameException | IOException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }finally {
            if (connections != null) {
            	close(connections);
            }
        }
    }

    /**
     * Close all JMX connections.
     * 
     * @param connections
     */
    protected static void close(List<Pair<String, JMXConnector>> connections) {
        if (connections != null) {
            for (Pair<String, JMXConnector> connectorPair : connections) {
                final JMXConnector second=connectorPair.getSecond();
                try{ second.close(); }catch(IOException ignored){ }
            }
        }
    }

    protected static void operate(JMXServerOperation operation) throws SQLException {
    	if (operation == null) throwNullArgError("operation");
        try(PartitionAdmin admin = SIDriver.driver().getTableFactory().getAdmin()){
            operate(operation,admin.allServers());
        }catch(IOException e){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
//        List<PartitionServer> regionServers = SpliceUtils.getServers();
//        operate(operation, regionServers);
    }

    protected static void operateOnMaster(JMXServerOperation operation) throws SQLException {
        if (operation == null) throwNullArgError("operation");

        throw new UnsupportedOperationException("IMPLEMENT");
//        PartitionServer masterServer = SpliceUtils.getMasterServer();
//
//        String serverName = masterServer.getHostname();
//        String port = SpliceConstants.config.get("hbase.master.jmx.port","10101");
//        try {
//            JMXServiceURL url = new JMXServiceURL(String.format("service:jmx:rmi://%1$s/jndi/rmi://%1$s:%2$s/jmxrmi",serverName,port));
//            JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
//            operation.operate(Arrays.asList(Pair.newPair(serverName,jmxc)));
//        } catch (IOException | MalformedObjectNameException e) {
//            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
//        }
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
        } catch (SQLException e) {
            connection.rollback();
            throw new SQLException(sb.toString(), e);
        } finally {
            connection.close();
        }
        return result;
    }

    protected static ExecRow buildExecRow(ResultColumnDescriptor[] columns) throws SQLException {
        ExecRow template = new ValueRow(columns.length);
        try {
            DataValueDescriptor[] rowArray = new DataValueDescriptor[columns.length];
            for(int i=0;i<columns.length;i++){
                rowArray[i] = columns[i].getType().getNull();
            }
            template.setRowArray(rowArray);
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
        return template;
    }

}
