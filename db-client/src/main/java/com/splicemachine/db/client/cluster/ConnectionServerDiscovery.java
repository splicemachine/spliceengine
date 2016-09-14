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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Scott Fines
 *         Date: 9/14/16
 */
public class ConnectionServerDiscovery implements ServerDiscovery{
    private static final Logger LOGGER=Logger.getLogger(ClusteredDataSource.class.getName());
    private final String[] initialServers;
    private final ServerList serverList;

    public ConnectionServerDiscovery(String[] initialServers,ServerList serverList){
        this.initialServers=initialServers;
        this.serverList=serverList;
    }

    @Override
    public List<String> detectServers() throws SQLException{
        Connection conn = getConnection();

        if(conn==null){
            LOGGER.log(Level.SEVERE,"Unable to perform service discovery: Unable to acquire connection");
            return null;
        }

        return fetchActiveServers(conn);
    }

    protected List<String> fetchActiveServers(Connection conn){
        try(CallableStatement cs=conn.prepareCall("call SYSCS_UTIL.GET_ACTIVE_SERVERS()")){
            try(ResultSet rs=cs.executeQuery()){
                List<String> newPool=new ArrayList<>();
                while(rs.next()){
                    String hostPort=rs.getString(1)+":"+rs.getString(2);

                    newPool.add(hostPort);
                }

                return newPool;
            }
        }catch(SQLException se){
            logError("serviceDiscovery",se);
        }
    }

    /* **********************************************************************************/
    /*private helper methods*/
    private Connection getConnection(){
        Connection conn = null;
        Iterator<ServerPool> serverIter = serverList.activeServers();


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
