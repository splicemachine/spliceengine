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
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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

    private final ServerList serverList;
    private final ServerPoolFactory poolFactory;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final boolean validateConnections;

    private final ScheduledExecutorService maintainer;
    private final ServerDiscovery serverDiscovery;
    private final long discoveryWindow;
    private final long heartbeatWindow;

    private final AtomicReference<FutureTask<Void>> discoveryTask = new AtomicReference<>(null);

    public static ClusteredDataSourceBuilder newBuilder(){
        return new ClusteredDataSourceBuilder();
    }

    ClusteredDataSource(ServerList serverList,
                        ServerPoolFactory poolFactory,
                        ServerDiscovery serverDiscovery,
                        ScheduledExecutorService maintainer,
                        boolean validateConnections,
                        long discoveryWindow,long heartbeatWindow){
        this.serverList = serverList;
        this.poolFactory = poolFactory;
        this.validateConnections = validateConnections;
        this.maintainer = maintainer;
        this.discoveryWindow=discoveryWindow;
        this.serverDiscovery = serverDiscovery;
        this.heartbeatWindow = heartbeatWindow;
    }

    public void start(){
        Discovery command=new Discovery();
        schedule(command);
    }


    @Override
    public Connection getConnection() throws SQLException{
        if(closed.get())
            throw new SQLException("DataSource is closed",SQLState.ALREADY_CLOSED);
        Connection conn=tryAcquire();
        if(conn!=null) return conn;

        /*
         * If we get to this point, then we tried to get a connection from an underlying pool,
         * but all pools are currently full. To that end, we go back to the original server pool
         * list and attempt to get a new connection; if that doesn't work, then we will fail
         */
        Iterator<ServerPool> activeServers = serverList.activeServers(); //get a new list
        while(activeServers.hasNext()){
            ServerPool serverPool = activeServers.next();
            try{
                conn = serverPool.tryAcquireConnection(validateConnections); //try the pool one more time
                if(conn!=null) return conn;
                else return serverPool.newConnection(); //try to get a new connection, but if that fails we keep trying
            }catch(SQLException se){
                if(!ClientErrors.isNetworkError(se)){
                    throw se;
                }
                logError("getConnection("+serverPool.serverName+")",se,Level.INFO);
                if(serverPool.isDead())
                    serverList.serverDead(serverPool);
            }
        }

        throw new SQLException("Unable to acquire a connection to any server: blacklisted nodes:"+serverList.blacklist(),SQLState.NO_CURRENT_CONNECTION);
    }


    @Override
    public Connection getConnection(String username,String password) throws SQLException{
        throw new SQLFeatureNotSupportedException("Username and password must be set when datasource is constructed");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException{
        throw new SQLFeatureNotSupportedException("unwrap");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException{
        return false;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException{
        throw new SQLFeatureNotSupportedException("getLogWriter");
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException{
        throw new SQLFeatureNotSupportedException("setLogWriter");
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException{
        throw new SQLFeatureNotSupportedException("setLoginTimeout"); //TODO -sf- implement this
    }

    @Override
    public int getLoginTimeout() throws SQLException{
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException{
        throw new SQLFeatureNotSupportedException("getParentLogger");
    }

    public void close() throws SQLException{
        if(!closed.compareAndSet(false,true)) return;
        ServerPool[] clear=serverList.clear();
        SQLException se = null;
        for(ServerPool sp:clear){
            try{
                sp.close();
            }catch(SQLException e){
                if(se==null) se = e;
                else se.setNextException(e);
            }
        }
        if(se!=null)
            throw se;
    }

    @SuppressWarnings("WeakerAccess")
    public void detectServers() throws SQLException{
        performDiscovery(new Discovery());
    }

    private void logError(String operation,Throwable t,Level logLevel){
        String errorMessage="error during "+operation+":";
        if(t instanceof SQLException){
            SQLException se=(SQLException)t;
            errorMessage+="["+se.getSQLState()+"]";
        }
        if(t.getMessage()!=null)
            errorMessage+=t.getMessage();

        LOGGER.log(logLevel,errorMessage,t);
    }

    private class Discovery implements Runnable,Callable<Void>{
        @Override
        public void run(){
            performDiscovery(this);
        }

        @Override
        public Void call() throws Exception{
            List<String> newServers = serverDiscovery.detectServers();
            ServerPool[] servers = new ServerPool[newServers.size()];
            int i=0;
            for(String server:newServers){
                ServerPool serverPool=poolFactory.newServerPool(server);
                servers[i] =serverPool;
                if(heartbeatWindow>0)
                    maintainer.schedule(new Heartbeat(serverPool),heartbeatWindow,TimeUnit.MILLISECONDS);
                i++;
            }
            serverList.setServerList(servers);
            return null;
        }
    }

    private Connection tryAcquire() throws SQLException{
        Iterator<ServerPool> activeServers = serverList.activeServers();
        while(activeServers.hasNext()){
            ServerPool serverPool = activeServers.next();
            try{
                Connection conn = serverPool.tryAcquireConnection(validateConnections);
                if(conn!=null) return conn;
            }catch(SQLException se){
                if(!ClientErrors.isNetworkError(se)){
                    throw se;
                }
                logError("getConnection("+serverPool.serverName+")",se,Level.INFO);
                if(serverPool.isDead())
                    serverList.serverDead(serverPool);
            }
        }
        return null;
    }

    private void performDiscovery(Callable<Void> callable){
        FutureTask<Void> running;
        do{
            running = discoveryTask.get();
            if(running!=null){
                try{
                    running.get();
                }catch(InterruptedException e){
                    Thread.currentThread().interrupt();
                }catch(ExecutionException e){
                    //this shouldn't happen, but just in case, we'll re-throw
                    throw new RuntimeException(e.getCause());
                }
                return;
            }else{
                FutureTask<Void> pTask =new FutureTask<>(callable);
                if(discoveryTask.compareAndSet(null,pTask)){
                    running = pTask;
                    break;
                }
            }
        }while(true);
        running.run();
        discoveryTask.set(null);
    }

    private void schedule(Runnable command){
        maintainer.scheduleAtFixedRate(command,
                0L,discoveryWindow,
                TimeUnit.MILLISECONDS);
    }

    private class Heartbeat implements Runnable{
        private ServerPool server;

        Heartbeat(ServerPool server){
            this.server=server;
        }

        @Override
        public void run(){
            server.heartbeat();
            if(server.isDead()){
                try{
                    serverList.serverDead(server);
                }catch(SQLException ignored){ } //ignore the exception cause we don't really care
            }else{
                maintainer.schedule(this,heartbeatWindow,TimeUnit.MILLISECONDS);
            }

        }
    }
}
