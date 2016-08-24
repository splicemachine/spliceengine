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
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.sql.DataSource;
import java.net.ConnectException;
import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 *         Date: 8/16/16
 */
public class ClusteredDataSourceTest{

    @Test
    public void canGetConnectionFromSingleServer() throws Exception{
       PoolSizingStrategy pss = new PoolSizingStrategy(){
           @Override public void acquirePermit(){ }
           @Override public void releasePermit(){ }

           @Override
           public int singleServerPoolSize(){
               return 10;
           }
       };

        ConnectionSelectionStrategy css = ConnectionStrategy.ROUND_ROBIN;

        FailureDetectorFactory failureDetectorFactory=new FailureDetectorFactory(){
            @Override public FailureDetector newFailureDetector(){
                //don't have the failure detector advance
                return new DeadlineFailureDetector(10){
                    @Override protected long currentTime(){ return 1L; }
                };
            }
        };
        ServerPoolFactory spf = new ConfiguredServerPoolFactory(failureDetectorFactory){
            @Override
            protected DataSource newDataSource(String serverId,
                                               String database,
                                               String user,
                                               String password){
                DataSource ds = mock(DataSource.class);
                try{
                    when(ds.getConnection()).thenReturn(mock(Connection.class));
                    when(ds.getConnection(anyString(),anyString())).thenReturn(mock(Connection.class));
                }catch(SQLException e){
                    throw new RuntimeException(e);
                }
                return ds;
            }
        };
        ClusteredDataSource cds = new ClusteredDataSource(new String[]{"testServer:1527"},"test","tu","tp",pss,css,spf,0,0);
        Connection c = cds.getConnection();
        Assert.assertNotNull("Did not get a non-null connection!",c);
    }

    @Test
    public void throwsErrorWithNoServers() throws Exception{
        PoolSizingStrategy pss = new PoolSizingStrategy(){
            @Override public void acquirePermit(){ }
            @Override public void releasePermit(){ }
            @Override public int singleServerPoolSize(){ return 10; }
        };

        ConnectionSelectionStrategy css = ConnectionStrategy.ROUND_ROBIN;

        FailureDetectorFactory failureDetectorFactory=new FailureDetectorFactory(){
            @Override public FailureDetector newFailureDetector(){
                //don't have the failure detector advance
                return new DeadlineFailureDetector(10){
                    @Override protected long currentTime(){ return 1L; }
                };
            }
        };
        ServerPoolFactory spf = new ConfiguredServerPoolFactory(failureDetectorFactory){
            @Override
            protected DataSource newDataSource(String serverId,
                                               String database,
                                               String user,
                                               String password){
                DataSource ds = mock(DataSource.class);
                try{
                    when(ds.getConnection()).thenReturn(mock(Connection.class));
                    when(ds.getConnection(anyString(),anyString())).thenReturn(mock(Connection.class));
                }catch(SQLException e){
                    throw new RuntimeException(e);
                }
                return ds;
            }
        };
        ClusteredDataSource cds = new ClusteredDataSource(new String[]{},"test","tu","tp",pss,css,spf,0,0);
        try{
            cds.getConnection();
            Assert.fail("Did not throw exception!");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect SQLState!",
                    SQLState.AUTH_DATABASE_CONNECTION_REFUSED,se.getSQLState());
        }
    }

    @Test
    public void throwsErrorWithDerbyConnectionRefused() throws Exception{
        PoolSizingStrategy pss = new PoolSizingStrategy(){
            @Override public void acquirePermit(){ }
            @Override public void releasePermit(){ }
            @Override public int singleServerPoolSize(){ return 10; }
        };

        ConnectionSelectionStrategy css = ConnectionStrategy.ROUND_ROBIN;

        FailureDetectorFactory failureDetectorFactory=new FailureDetectorFactory(){
            @Override public FailureDetector newFailureDetector(){
                //don't have the failure detector advance
                return new DeadlineFailureDetector(10){
                    @Override protected long currentTime(){ return 1L; }
                };
            }
        };
        //in this case, we want to use derby's logic so that we know we actually try to connect
        ServerPoolFactory spf = new ConfiguredServerPoolFactory(failureDetectorFactory);
        ClusteredDataSource cds = new ClusteredDataSource(new String[]{"localhost:60000"},"test","tu","tp",pss,css,spf,0,0);
        try{
            cds.getConnection();
            Assert.fail("Did not throw exception!");
        }catch(SQLException se){
            se.printStackTrace();
            Assert.assertEquals("Incorrect SQLState!",
                    SQLState.AUTH_DATABASE_CONNECTION_REFUSED,se.getSQLState());
        }
    }

    @Test
    public void blacklistsServerWithConnectionRefused() throws Exception{
        PoolSizingStrategy pss = new PoolSizingStrategy(){
            @Override public void acquirePermit(){ }
            @Override public void releasePermit(){ }
            @Override public int singleServerPoolSize(){ return 10; }
        };

        ConnectionSelectionStrategy css = new ConnectionSelectionStrategy(){
            @Override public int nextServer(int previous,int numServers){ return 0; }
        };

        FailureDetectorFactory failureDetectorFactory=new FailureDetectorFactory(){
            @Override public FailureDetector newFailureDetector(){
                //don't have the failure detector advance
                return new DeadlineFailureDetector(10){
                    @Override protected long currentTime(){ return 1L; }
                };
            }
        };
        //in this case, we want to use derby's logic so that we know we actually try to connect
        ServerPoolFactory spf = new ConfiguredServerPoolFactory(failureDetectorFactory){
            @Override
            protected DataSource newDataSource(String serverId,
                                               String database,
                                               String user,
                                               String password){
                DataSource ds=mock(DataSource.class);
                if(serverId.equals("failServer")){
                    SQLNonTransientConnectionException e = new SQLNonTransientConnectionException(new ConnectException("Connection refused"));
                    try{
                        when(ds.getConnection(anyString(),anyString())).thenThrow(e);
                        when(ds.getConnection()).thenThrow(e);
                    }catch(SQLException se){
                        throw new RuntimeException(se);
                    }
                }else{
                    Connection conn = mock(Connection.class);
                    try{
                        when(conn.isClosed()).thenReturn(false);
                        when(ds.getConnection(anyString(),anyString())).thenReturn(conn);
                        when(ds.getConnection()).thenReturn(conn);
                    }catch(SQLException se){
                        throw new RuntimeException(se);
                    }
                }
                return ds;
            }
        };
        ClusteredDataSource cds = new ClusteredDataSource(new String[]{"failServer","successServer"},"test","tu","tp",pss,css,spf,0,0);
        Connection c=cds.getConnection();
        Assert.assertNotNull("Returned a null connection!",c);
        Assert.assertFalse("Returned a closed connection!",c.isClosed());


        Set<String> blacklistedServers=cds.blacklistedServers();
        Assert.assertEquals("Incorrect # of blacklisted servers",1,blacklistedServers.size());
        Assert.assertTrue("Did not contain correct server!",blacklistedServers.contains("failServer"));
    }

    @Test
    public void serviceDiscoveryAddsServer() throws Exception{
        PoolSizingStrategy pss = new PoolSizingStrategy(){
            @Override public void acquirePermit(){ }
            @Override public void releasePermit(){ }
            @Override public int singleServerPoolSize(){ return 10; }
        };

        ConnectionSelectionStrategy css = ConnectionStrategy.ROUND_ROBIN;

        FailureDetectorFactory failureDetectorFactory=new FailureDetectorFactory(){
            @Override public FailureDetector newFailureDetector(){
                //don't have the failure detector advance
                return new DeadlineFailureDetector(10){
                    @Override protected long currentTime(){ return 1L; }
                };
            }
        };
        //in this case, we want to use derby's logic so that we know we actually try to connect
        ServerPoolFactory spf = new ConfiguredServerPoolFactory(failureDetectorFactory){
            @Override
            protected DataSource newDataSource(String serverId,
                                               String database,
                                               String user,
                                               String password){
                final Iterator<String> servers = Arrays.asList("newServer","oldServer").iterator();
                ResultSet rs = mock(ResultSet.class);
                Statement s = mock(Statement.class);
                Connection conn = mock(Connection.class);
                DataSource ds = mock(DataSource.class);
                try{
                    when(rs.next()).thenAnswer(new Answer<Boolean>(){
                        @Override
                        public Boolean answer(InvocationOnMock invocation) throws Throwable{
                            return servers.hasNext();
                        }
                    });
                    when(rs.getString(1)).thenAnswer(new Answer<String>(){
                        @Override
                        public String answer(InvocationOnMock invocation) throws Throwable{
                            return servers.next();
                        }
                    });
                    when(s.executeQuery(ClusteredDataSource.DEFAULT_ACTIVE_SERVER_QUERY)).thenReturn(rs);
                    when(conn.createStatement()).thenReturn(s);
                    when(ds.getConnection()).thenReturn(conn);
                    when(ds.getConnection(anyString(),anyString())).thenReturn(conn);
                }catch(SQLException se){
                    throw new RuntimeException(se);
                }
                return ds;
            }
        };
        ClusteredDataSource cds = new ClusteredDataSource(new String[]{"oldServer"},"test","tu","tp",pss,css,spf,0,0);

        cds.performServiceDiscovery();
        Set<String> activeServers = cds.activeServers();
        Assert.assertEquals("Did not add a new server!",2,activeServers.size());
        Assert.assertTrue("Did not contain oldServer!",activeServers.contains("oldServer"));
        Assert.assertTrue("Did not contain newServer!",activeServers.contains("newServer"));
    }

    @Test
    public void serviceDiscoveryRemovesServer() throws Exception{
        PoolSizingStrategy pss = new PoolSizingStrategy(){
            @Override public void acquirePermit(){ }
            @Override public void releasePermit(){ }
            @Override public int singleServerPoolSize(){ return 10; }
        };

        ConnectionSelectionStrategy css = ConnectionStrategy.ROUND_ROBIN;

        FailureDetectorFactory failureDetectorFactory=new FailureDetectorFactory(){
            @Override public FailureDetector newFailureDetector(){
                //don't have the failure detector advance
                return new DeadlineFailureDetector(10){
                    @Override protected long currentTime(){ return 1L; }
                };
            }
        };
        //in this case, we want to use derby's logic so that we know we actually try to connect
        ServerPoolFactory spf = new ConfiguredServerPoolFactory(failureDetectorFactory){
            @Override
            protected DataSource newDataSource(String serverId,
                                               String database,
                                               String user,
                                               String password){
                DataSource ds = mock(DataSource.class);
                if(serverId.equals("deadServer")){
                    try{
                        Answer<Connection> answer=new Answer<Connection>(){
                            @Override
                            public Connection answer(InvocationOnMock invocation) throws Throwable{
                                Assert.fail("Should not get a connection from deadServer!");
                                return null;
                            }
                        };
                        when(ds.getConnection()).then(answer);
                        when(ds.getConnection(anyString(),anyString())).then(answer);
                    }catch(SQLException se){
                        throw new RuntimeException(se);
                    }
                }else{
                    final Iterator<String> servers=Collections.singletonList("oldServer").iterator();
                    ResultSet rs=mock(ResultSet.class);
                    Statement s=mock(Statement.class);
                    Connection conn=mock(Connection.class);
                    try{
                        when(rs.next()).thenAnswer(new Answer<Boolean>(){
                            @Override
                            public Boolean answer(InvocationOnMock invocation) throws Throwable{
                                return servers.hasNext();
                            }
                        });
                        when(rs.getString(1)).thenAnswer(new Answer<String>(){
                            @Override
                            public String answer(InvocationOnMock invocation) throws Throwable{
                                return servers.next();
                            }
                        });
                        when(s.executeQuery(ClusteredDataSource.DEFAULT_ACTIVE_SERVER_QUERY)).thenReturn(rs);
                        when(conn.createStatement()).thenReturn(s);
                        when(ds.getConnection()).thenReturn(conn);
                        when(ds.getConnection(anyString(),anyString())).thenReturn(conn);
                    }catch(SQLException se){
                        throw new RuntimeException(se);
                    }
                }
                return ds;
            }
        };
        ClusteredDataSource cds = new ClusteredDataSource(new String[]{"deadServer","oldServer"},"test","tu","tp",pss,css,spf,0,0);

        cds.performServiceDiscovery();
        Set<String> activeServers = cds.activeServers();
        Assert.assertEquals("Did not remove deadServer!",1,activeServers.size());
        Assert.assertTrue("Did not contain oldServer!",activeServers.contains("oldServer"));
        Assert.assertFalse("Still contains deadServer!",activeServers.contains("deadServer"));

        //get a few connections just to make sure that we don't ever call newDataSource
        for(int i=0;i<5;i++){
            Connection conn = cds.getConnection();
            conn.close();
        }
    }
}