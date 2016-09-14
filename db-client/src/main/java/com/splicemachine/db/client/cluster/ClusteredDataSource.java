/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.cluster;

import com.splicemachine.db.iapi.reference.SQLState;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Maintains a pool of active connections to a cluster of servers. This does <em>not</em>
 * manage direct network access, only the connections themselves.
 * <p>
 * The logic is relatively straightforward: when asked for a connection, the pool
 * will provide one to one of the servers in the cluster (depending on the access strategy chosen);
 * that connection will be guaranteed to be active (within the allowed semantics described below),
 * and cannot be used by any other thread while the connection is allowed.
 * <p>
 * If no connections are available, then the pool has a choice: you can forcibly block the requesting
 * thread until a connection becomes available (i.e. a limited pool size), or you can create a new
 * connection. The first option requires fewer cluster and client resources, but may impose a performance
 * penalty; the second option reverse the choice (using more resources in exchange for higher concurrency).
 * In general, however, it is usually best to choose option 1, and to size your pool appropriately.
 * <p>
 * Adding new nodes:
 * <p>
 * The method {@link #detectServers()} will be called periodically (from a scheduled thread).
 * By default, this method grabs an existing connection, and then calls the SYSCS_UTIL.DETECT_SERVERS()
 * stored procedure; This procedure is expected to list the servers which are available to connect to:
 * <p>
 * If there are new servers in this list, then they will be automatically added to the pool and made
 * available for selection.
 * <p>
 * If there are servers that used to be in the list, but are no longer available, then those servers
 * will be added to the "blacklist", and any outstanding connections to that server will be killed. If
 * any of those connections are currently in use, those connections will be considered "closed" and will
 * throw an error when an attempt is made to perform any operation with them.
 * <p>
 * Heartbeats:
 * <p>
 * To ensure that a given server is available to this node, we make use of "heartbeats": Periodically,
 * we will emit a short SQL command ({@link ServerPool#heartbeat()}) to each server in the server's white
 * and black lists. If the server responds, then we consider this a successful heartbeat; otherwise, it
 * is a failed heartbeat. If it failed a sufficient number of heartbeats within a (configurable) time window,
 * then it is probabilistically declared dead.
 * <p>
 * The heartbeat() command can be anything: however, for failure-detecting purposes, it is generally best
 * to make the heartbeat operation as simple as possible. Performing additional work during the heartbeat
 * period may lead to nodes being treated as down spuriously.
 * <p>
 * Detecting "down" nodes:
 * <p>
 * A node is considered "down" if one of the following conditions holds:
 * <p>
 * 1. The server is removed from the detectServers() method call's return list
 * 2. A sufficient number of heartbeats fail within the heartbeat window
 * 3. A sufficeint number of connection attempts are outright refused. For example, if a connection attempt
 * fails 3 times with a ConnectionRefused, then there is little reason for us to assume it is anything
 * other than dead.
 * <p>
 * Cleaning unused connections:
 * <p>
 * Usage patterns often vary over time, so it is not surprising that we may reach a slow-down in usage patterns.
 * When that happens, it is often desirable to automatically close connections which haven't been used in a sufficient
 * amount of time (i.e. allowing the pool to shrink). This reclaims resources well, but does impose a performance
 * problem when usage patterns ramp up again; As a result, it is possible to disable this.
 *
 * @author Scott Fines
 *         Date: 8/15/16
 */
public class ClusteredDataSource implements DataSource{
    /*
     * We use java.util.logging here to avoid requiring a logging jar dependency on our applications (and therefore
     * causing all kinds of potential dependency problems).
     */
    private static final Logger LOGGER = Logger.getLogger(ClusteredDataSource.class.getName());

    @Override
    public Connection getConnection() throws SQLException{
        return null;
    }

    @Override
    public Connection getConnection(String username,String password) throws SQLException{
        throw new SQLFeatureNotSupportedException("Username and password must be set when datasource is constructed");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException{
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException{
        return false;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException{
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException{

    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException{

    }

    @Override
    public int getLoginTimeout() throws SQLException{
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException{
        return null;
    }

    public void close(){

    }
}
