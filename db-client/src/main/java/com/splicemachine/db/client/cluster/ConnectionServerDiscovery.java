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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Scott Fines
 *         Date: 9/14/16
 */
class ConnectionServerDiscovery implements ServerDiscovery{
    private static final Logger LOGGER=Logger.getLogger(ConnectionServerDiscovery.class.getName());
    static final String DEFAULT_ACTIVE_SERVER_QUERY="call SYSCS_UTIL.SYSCS_GET_ACTIVE_SERVERS()";
    private final String[] initialServers;
    private final ServerList serverList;
    private final ServerPoolFactory poolFactory;

    ConnectionServerDiscovery(String[] initialServers,
                              ServerList serverList,
                              ServerPoolFactory poolFactory){
        this.initialServers=initialServers;
        this.serverList=serverList;
        this.poolFactory = poolFactory;
    }

    @Override
    public List<String> detectServers() throws SQLException{
        try(Connection conn = getConnection()){

            if(conn==null){
                LOGGER.log(Level.SEVERE,"Unable to perform service discovery: Unable to acquire connection");
                return null;
            }

            if(LOGGER.isLoggable(Level.FINEST))
                LOGGER.finest("Performing Service Discovery with connection "+conn);
            return fetchActiveServers(conn);
        }
    }

    @SuppressWarnings("WeakerAccess") //designed for overridding
    protected List<String> fetchActiveServers(Connection conn) throws SQLException{
        try(Statement cs=conn.createStatement()){
            try(ResultSet rs=cs.executeQuery(DEFAULT_ACTIVE_SERVER_QUERY)){
                List<String> newPool=new ArrayList<>();
                while(rs.next()){
                    String hostPort=rs.getString(1)+":"+rs.getString(2);

                    newPool.add(hostPort);
                }

                return newPool;
            }
        }
    }

    /* **********************************************************************************/
    /*private helper methods*/

    private /*@Nullable*/ Connection getConnection(){
        Connection conn;
        Iterator<ServerPool> serverIter = serverList.activeServers();
        boolean[] visitedInitialServers = new boolean[initialServers.length];
        while(serverIter.hasNext()){
            ServerPool next=serverIter.next();
            try{
                conn=next.tryAcquireConnection(true);
                if(conn!=null){
                    if(LOGGER.isLoggable(Level.FINEST))
                        LOGGER.finest("Obtained pooled connection "+ conn+" from active server "+ next);
                    return conn;
                }
            }catch(SQLException se){
                logError("getConnection("+next.serverName+")",se);
            }
            for(int i=0;i<initialServers.length;i++){
                String server = initialServers[i];
                if(server.startsWith(next.serverName)){
                    visitedInitialServers[i] = true;
                    break;
                }
            }
        }

        //we couldn't acquire a connection from the pool, so try and force create one
        serverIter = serverList.activeServers();
        while(serverIter.hasNext()){
            ServerPool next=serverIter.next();
            try{
                conn=next.newConnection();
                if(conn!=null){
                    if(LOGGER.isLoggable(Level.FINEST))
                        LOGGER.finest("Obtained new connection "+ conn+" from active server "+ next);
                    return conn;
                }
            }catch(SQLException se){
                logError("getConnection("+next.serverName+")",se);
            }
            for(int i=0;i<initialServers.length;i++){
                String server = initialServers[i];
                if(server.startsWith(next.serverName)){
                    visitedInitialServers[i] = true;
                    break;
                }
            }
        }

        /*
         * Being here means that either
         * A) there are no servers in the server list or
         * B) all the servers broke when we tried to create a new connection
         *
         * We'll be gracious and assume that there were no servers, and choose a server
         * from the initial list
         */
        for(int i=0;i<initialServers.length;i++){
            if(!visitedInitialServers[i]){
                ServerPool newPool = poolFactory.newServerPool(initialServers[i]);
                try{
                    conn=newPool.tryAcquireConnection(false);
                    if(conn==null)
                        conn = newPool.newConnection();
                    if(conn!=null) {
                        if(LOGGER.isLoggable(Level.FINEST)){
                            LOGGER.finest("initial server "+ newPool+" is alive, adding");
                        }
                        serverList.addServer(newPool); //this server is actually active, so we can re-use this
                        return conn;
                    }else{
                        try{
                            if(LOGGER.isLoggable(Level.FINEST)){
                                LOGGER.finest("initial server "+ newPool+" is not alive, closing pool");
                            }
                            newPool.close();
                        }catch(SQLException e){
                            logError("serverPool.close("+newPool.serverName+")",e);
                        }
                    }
                }catch(SQLException se){
                    logError("getConnection("+newPool.serverName+")",se);
                }
            }
        }

        /*
         * We got here which means that we weren't able to find anything on our initial
         * server list either. Not much to do except return null
         */
        return null;
    }

    private void logError(String operation,Throwable t){
        logError(operation,t,Level.SEVERE);
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
}
