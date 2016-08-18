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

    private final ConnectionSelectionStrategy selectionStrategy;
    private final PoolSizingStrategy sizingStrategy;

    private final List<ServerPool> activeServers=new CopyOnWriteArrayList<>();
    private final AtomicInteger serverChoice=new AtomicInteger(0);

    private final BlackList<ServerPool> blackListedServers=new BlackList<ServerPool>(){
        @Override
        protected void cleanupResources(ServerPool element){
            cleanupDeadServer(element);
        }
    };

    private volatile int loginTimeout=0;
    private final ScheduledExecutorService maintenanceThread;
    private final ServerPoolFactory serverPoolFactory;
    private final long heartbeatInterval;

    public ClusteredDataSource(String[] initialServers,
                               PoolSizingStrategy poolSizingStrategy,
                               ConnectionSelectionStrategy selectionStrategy,
                               ServerPoolFactory serverPoolFactory,
                               long heartbeatPeriod,
                               long serverCheckPeriod){
        this.selectionStrategy=selectionStrategy;
        this.sizingStrategy=poolSizingStrategy;
        this.serverPoolFactory = serverPoolFactory;
        this.heartbeatInterval = heartbeatPeriod;
        this.maintenanceThread=Executors.newSingleThreadScheduledExecutor(new ThreadFactory(){
            @Override
            @SuppressWarnings("NullableProblems")
            public Thread newThread(Runnable r){
                Thread t=new Thread(r);
                t.setDaemon(true);
                t.setName("ClusterDataSource-maintainer");
                return t;
            }
        });

        setupServers(initialServers,heartbeatPeriod);
        submitMaintenance(serverCheckPeriod);
    }


    @Override
    public Connection getConnection() throws SQLException{
        return getConnection(null,null);
    }

    @Override
    public Connection getConnection(String username,String password) throws SQLException{
        if(activeServers.size()<=0)
            throw new SQLException("There are no active and available servers; blacklisted nodes:"+blackListedServers.toString(),
                    SQLState.AUTH_DATABASE_CONNECTION_REFUSED,1);

        sizingStrategy.acquirePermit();

        int serverPos;
        boolean shouldContinue;
        do{
            int previous=serverChoice.get();
            serverPos=selectionStrategy.nextServer(previous,activeServers.size());

            shouldContinue=!serverChoice.compareAndSet(previous,serverPos);
        }while(shouldContinue);

        int numtries=activeServers.size();
        int p=serverPos;
        while(numtries>0){

            /*
             * We take a modulo here because it is possible that the activeServers list
             * was changed between when we made the successful CAS operation above and
             * when we reached this point; this means the list may have shrunk, so we need
             * to adjust for that. It's *HIGHLY* unlikely to occur, but safety is our
             * first priority!
             */
            ServerPool sp=activeServers.get(p%activeServers.size());
            if(sp.isDead()){
                cleanupDeadServer(sp);
            }else{
                Connection conn=sp.tryAcquireConnection(username,password);
                if(conn!=null)
                    return conn;
            }

            p=p+1;
            numtries--;
        }
        /*
         * If we reached here, we couldn't find any active Connections to work with,
         * which means we should create a new one. We create a new one at the specified
         * serverPos
         *
         * Normally we wouldn't catch the runtime exceptions here, since they are a programemr
         * error. However, there is an inherent race condition: we are performing an unsynchronized
         * read-then-write operation here (reading the size of the list, then performing an access against it).
         * This means that we could read the size of the list, only to have the list shrink out from under us,
         * causing one of two potential problems: an ArrayIndexOutOfBoundsException(if we are looking off the end
         * of the list), or an ArithmeticException (if the server size is zero). In the event of the server size
         * being zero, we want to fail right away, but an IndexOutOfBounds should just trigger another read attempt
         */
        while(true){
            try{
                return activeServers.get(serverPos%activeServers.size()).newConnection(username,password);
            }catch(ArithmeticException ae){
                throw new SQLException("There are no active and available servers; blacklisted nodes:"+blackListedServers.toString(),
                        SQLState.AUTH_DATABASE_CONNECTION_REFUSED,1);
            }catch(IndexOutOfBoundsException ignored){ }
        }
    }


    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException{
        throw new SQLException("Unable to unwrap interface: "+iface,SQLState.UNABLE_TO_UNWRAP,1);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException{
        return false;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException{
        throw new SQLFeatureNotSupportedException("getLogWriter not supported");
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException{
        throw new SQLFeatureNotSupportedException("setLogWriter not supported");
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException{
        throw new SQLFeatureNotSupportedException("getParentLogger not supported");
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException{
        this.loginTimeout=seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException{
        return loginTimeout;
    }

    @SuppressWarnings("WeakerAccess")
    public Set<String> blacklistedServers(){
        return blackListedServers.currentBlacklist();
    }

    public Set<String> activeServers(){
        Set<String> activeServerNames = new HashSet<>(activeServers.size());
        for(ServerPool sp:activeServers){
           activeServerNames.add(sp.serverName);
        }
        return activeServerNames;
    }

    static final String DEFAULT_ACTIVE_SERVER_QUERY="call SYSCS_UTIL.GET_ACTIVE_SERVERS()";

    @SuppressWarnings("WeakerAccess")
    protected Set<String> detectServers(){
        int numTries=activeServers.size();
        do{
            try(Connection conn=getConnection()){
                try(Statement s=conn.createStatement()){
                    try(ResultSet rs=s.executeQuery(DEFAULT_ACTIVE_SERVER_QUERY)){
                        Set<String> servers=new TreeSet<>();
                        while(rs.next()){
                            servers.add(rs.getString(1));
                        }
                        return servers;
                    }
                }
            }catch(SQLException se){
                logError("detectServers()",se);
                numTries--;
            }
        }while(numTries>0);
        return Collections.emptySet();
    }

    @SuppressWarnings("WeakerAccess")
    public void performServiceDiscovery(){
        Set<String> servers=detectServers();
        //remove dead nodes
        for(ServerPool sp : activeServers){
            if(servers.remove(sp.serverName))
                continue; //this server is still active, nothing to worry about

            blackListedServers.blacklist(sp);
            /*
             * This is safe only as long as we are using
             * one of that java.util.concurrent collections, which makes use of weak
             * iteration and doesn't prohibit concurrent modification.
             */
            activeServers.remove(sp);
        }
            /*
             * Servers still in the server list are "new", in the sense that they
             * were either dead (and came back alive), or are newly added to the cluster.
             * Either way, add them in to the active servers list
             */
        for(String newServer:servers){
            ServerPool sp = newServerPool(newServer);
            activeServers.add(sp);
            blackListedServers.whitelist(sp);

            startHeartbeat(heartbeatInterval,sp);
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    private void setupServers(String[] initialServers,long heartbeatInterval){
        for(String server:initialServers){
            ServerPool sp = newServerPool(server);
            activeServers.add(sp);
            startHeartbeat(heartbeatInterval,sp);
        }
    }

    private void startHeartbeat(long heartbeatInterval,ServerPool sp){
        if(heartbeatInterval>0)
            maintenanceThread.schedule(new Heartbeat(sp,heartbeatInterval),heartbeatInterval,TimeUnit.MILLISECONDS);
    }

    private void submitMaintenance(long serverCheckPeriod){
        if(serverCheckPeriod>0){
            ServerDiscovery command=new ServerDiscovery(serverCheckPeriod);
            maintenanceThread.scheduleWithFixedDelay(command,0,serverCheckPeriod,TimeUnit.MILLISECONDS);
        }
    }

    private void cleanupDeadServer(ServerPool sp){
        blackListedServers.blacklist(sp);
        activeServers.remove(sp);
        try{
            sp.close();
        }catch(SQLException e){
            logError("close("+sp+")",e);
        }
    }

    private ServerPool newServerPool(String newServer){
        return serverPoolFactory.newServerPool(newServer,sizingStrategy,blackListedServers);
    }

    private void logError(String operation,SQLException se){
        LOGGER.log(Level.SEVERE,"error during "+operation+":"+se.toString(),se);
    }


    private class ServerDiscovery implements Runnable{
        private final long discoveryDelay;

        ServerDiscovery(long discoveryDelay){
            this.discoveryDelay=discoveryDelay;
        }

        @Override
        public void run(){
            performServiceDiscovery();

            if(LOGGER.isLoggable(Level.FINER))
                LOGGER.log(Level.FINER,"After server checks: blacklistedServers = {0}",blackListedServers);

            if(activeServers.size()<=0){
                /*
                 * We were not able to connect to ANY servers in the cluster: This is pretty catastrophic.
                 * We are going to move everyone to the blacklist as expected, but we are also going to
                 * shorten our retry schedule (so that we try again in half the normal maintenance period)
                 */
                maintenanceThread.schedule(this,discoveryDelay/2,TimeUnit.MILLISECONDS);
            }
        }
    }

    private class Heartbeat implements Runnable{
        private final ServerPool sp;
        private final long heartbeatInterval;

        Heartbeat(ServerPool sp,long heartbeatInterval){
            this.sp=sp;
            this.heartbeatInterval=heartbeatInterval;
        }

        @Override
        public void run(){
            try{
                if(sp.heartbeat()){
                    blackListedServers.whitelist(sp); //in case it had been blacklisted, but we are now alive again
                    maintenanceThread.schedule(this,heartbeatInterval,TimeUnit.MILLISECONDS);
                }else{
                    cleanupDeadServer(sp);
                }
            }catch(SQLException se){
                logError("heartbeat("+sp+")",se);
                cleanupDeadServer(sp);
            }
        }
    }
}
