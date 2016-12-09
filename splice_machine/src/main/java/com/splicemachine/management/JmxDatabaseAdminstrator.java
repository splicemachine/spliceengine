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
 */

package com.splicemachine.management;

import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.mbeans.drda.NetworkServerMBean;
import com.splicemachine.derby.management.StatementManagement;
import com.splicemachine.derby.utils.DatabasePropertyManagement;
import com.splicemachine.hbase.jmx.JMXUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.pipeline.threadpool.ThreadPoolStatus;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.PartitionServer;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.logging.Logging;

import javax.management.MalformedObjectNameException;
import javax.management.remote.JMXConnector;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 2/17/16
 */
public class JmxDatabaseAdminstrator implements DatabaseAdministrator{
    @Override
    public Map<String, Collection<Integer>> getJDBCHostPortInfo() throws SQLException{
        final Map<String,Collection<Integer>> retMap = new HashMap<>();
        try(PartitionAdmin admin = SIDriver.driver().getTableFactory().getAdmin()){
            Collection<PartitionServer> partitionServers=admin.allServers();
            Map<String,PartitionServer> serverHostMap = new HashMap<>(partitionServers.size());
            for(PartitionServer ps:partitionServers){
                serverHostMap.put(ps.getHostAndPort(),ps);
            }
            List<Pair<String,JMXConnector>> connections = null;
            try{
                connections = getConnections(partitionServers);
                Map<String, Collection<NetworkServerMBean>> networkServer=JMXUtils.getNetworkServer(connections);
                for(Map.Entry<String, Collection<NetworkServerMBean>> entry : networkServer.entrySet()){
                    Collection<Integer> ports=new ArrayList<>(entry.getValue().size());
                    for(NetworkServerMBean nsmb : entry.getValue()){
                        ports.add(nsmb.getDrdaPortNumber());
                    }

                    retMap.put(serverHostMap.get(entry.getKey()).getHostname(),ports);
                }
            }finally{
                if(connections!=null)
                    close(connections);
            }

        }catch(IOException | MalformedObjectNameException e){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
        return retMap;
    }

    @Override
    public void setLoggerLevel(final String loggerName,final String logLevel) throws SQLException{
        operate(new JMXServerOperation(){
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException{
                for(Logging logger : JMXUtils.getLoggingManagement(connections)){
                    logger.setLoggerLevel(loggerName,logLevel);
                }
            }
        });
    }

    @Override
    public List<String> getLoggerLevel(final String loggerName) throws SQLException{
        final List<String> loggers=new ArrayList<>();
        operate(new JMXServerOperation(){
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException{
                for(Logging logger : JMXUtils.getLoggingManagement(connections)){
                    loggers.add(logger.getLoggerLevel(loggerName));
                }
            }
        });
        return loggers;
    }

    @Override
    public Set<String> getLoggers() throws SQLException{
        final Set<String> loggers=new HashSet<>();
        operate(new JMXServerOperation(){
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException{
                for(Logging logger : JMXUtils.getLoggingManagement(connections)){
                    loggers.addAll(logger.getLoggerNames());
                }
            }
        });
        return loggers;
    }

    @Override
    public Map<String, DatabaseVersion> getClusterDatabaseVersions() throws SQLException{
        final Map<String, DatabaseVersion> dbVersions=new HashMap<>();
        operate(new JMXServerOperation(){
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException{
                List<DatabaseVersion> databaseVersions=JMXUtils.getSpliceMachineVersion(connections);
                int i=0;
                for(DatabaseVersion databaseVersion : databaseVersions){
                    dbVersions.put(connections.get(i).getFirst(),databaseVersion);
                }
            }
        });
        return dbVersions;
    }

    @Override
    public Map<String,Map<String,String>> getDatabaseVersionInfo() throws SQLException{
        final Map<String, Map<String,String>> versionInfo = new HashMap<>();
        operate(new JMXServerOperation(){
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException{
                List<DatabaseVersion> databaseVersions=JMXUtils.getSpliceMachineVersion(connections);
                int i=0;
                for(DatabaseVersion databaseVersion : databaseVersions){
                    Map<String,String> attrs = new HashMap<>();
                    attrs.put("release", databaseVersion.getRelease());
                    attrs.put("implementationVersion", databaseVersion.getImplementationVersion());
                    attrs.put("buildTime", databaseVersion.getBuildTime());
                    attrs.put("url", databaseVersion.getURL());
                    versionInfo.put(connections.get(i).getFirst(),attrs);
                    i++;
                }
            }
        });
        return versionInfo;
    }

    @Override
    public void setWritePoolMaxThreadCount(final int maxThreadCount) throws SQLException{
        operate(new JMXServerOperation(){
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException{
                List<ThreadPoolStatus> threadPools=JMXUtils.getMonitoredThreadPools(connections);
                for(ThreadPoolStatus threadPool : threadPools){
                    threadPool.setMaxThreadCount(maxThreadCount);
                }
            }
        });
    }

    @Override
    public Map<String,Integer> getWritePoolMaxThreadCount() throws SQLException{
        final Map<String,Integer> data = new HashMap<>();
        operate(new JMXServerOperation(){
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException{
                List<ThreadPoolStatus> threadPools=JMXUtils.getMonitoredThreadPools(connections);
                int i=0;
                for(ThreadPoolStatus threadPool : threadPools){
                    data.put(connections.get(i).getFirst(),threadPool.getMaxThreadCount());
                }
            }
        });
        return data;
    }

    @Override
    public Map<String,String> getGlobalDatabaseProperty(final String key) throws SQLException{
        final Map<String,String> data = new HashMap<>();
        operate(new JMXServerOperation(){
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException{
                List<DatabasePropertyManagement> dbPropManagement = JMXUtils.getDatabasePropertyManagement(connections);
                int i=0;
                for(DatabasePropertyManagement dpm:dbPropManagement){
                    data.put(connections.get(i).getFirst(),dpm.getDatabaseProperty(key));
                    i++;
                }
            }
        });
        return data;
    }

    @Override
    public void setGlobalDatabaseProperty(final String key,final String value) throws SQLException{
        operate(new JMXServerOperation(){
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException{
                for(DatabasePropertyManagement databasePropertyMgmt : JMXUtils.getDatabasePropertyManagement(connections)){
                    databasePropertyMgmt.setDatabaseProperty(key,value);
                }
            }
        });
    }

    @Override
    public void emptyGlobalStatementCache() throws SQLException{
        operate(new JMXServerOperation(){
            // This procedure is essentially a wrapper around the Derby stored proc SYSCS_EMPTY_STATEMENT_CACHE
            // such that it is done on every node in the cluster.
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException{
                List<Pair<String, StatementManagement>> statementManagers=JMXUtils.getStatementManagers(connections);
                for(Pair<String, StatementManagement> managementPair : statementManagers){
                    managementPair.getSecond().emptyStatementCache();
                }
            }
        });
    }

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private interface JMXServerOperation{
        void operate(List<Pair<String, JMXConnector>> jmxConnector) throws MalformedObjectNameException, IOException, SQLException;
    }

    /**
     * Get the JMX connections for the region servers.
     *
     * @param serverNames
     * @return
     * @throws IOException
     */
    private static List<Pair<String, JMXConnector>> getConnections(Collection<PartitionServer> serverNames) throws IOException{
        return JMXUtils.getMBeanServerConnections(getServerNames(serverNames));
    }

    private static void operate(JMXServerOperation operation) throws SQLException{
        if(operation==null) throwNullArgError("operation");
        try(PartitionAdmin admin=SIDriver.driver().getTableFactory().getAdmin()){
            operate(operation,admin.allServers());
        }catch(IOException e){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
    }

    /**
     * Execute (or "operate") the JMX operation on the region servers.
     * JMX connections will be created and closed for each operation on each region server.
     *
     * @param operation
     * @param serverNames
     * @throws SQLException
     */
    private static void operate(JMXServerOperation operation,Collection<PartitionServer> serverNames) throws SQLException{
        if(operation==null) throwNullArgError("operation");
        List<Pair<String, JMXConnector>> connections=null;
        try{
            connections=getConnections(serverNames);
            operation.operate(connections);
        }catch(MalformedObjectNameException|IOException e){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }finally{
            if(connections!=null){
                close(connections);
            }
        }
    }

    private static List<Pair<String, String>> getServerNames(Collection<PartitionServer> serverInfo){
        List<Pair<String, String>> names=new ArrayList<>(serverInfo.size());
        for(PartitionServer sname : serverInfo){
            names.add(Pair.newPair(sname.getHostname(),sname.getHostAndPort()));
        }
        return names;
    }

    /**
     * Close all JMX connections.
     *
     * @param connections
     */
    private static void close(List<Pair<String, JMXConnector>> connections){
        if(connections!=null){
            for(Pair<String, JMXConnector> connectorPair : connections){
                final JMXConnector second=connectorPair.getSecond();
                try{
                    second.close();
                }catch(IOException ignored){
                }
            }
        }
    }

    private static void throwNullArgError(Object value){
        throw new IllegalArgumentException(String.format("Required argument %s is null.",value));
    }
}
