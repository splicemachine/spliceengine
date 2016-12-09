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
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 *         Date: 8/16/16
 */
public class ClusteredDataSourceTest{

    private static final ScheduledExecutorService ses = mock(ScheduledExecutorService.class);
    private static final Answer<ScheduledFuture<?>> answer=invocation->{
        ((Runnable)invocation.getArguments()[0]).run();
        return null;
    };
    static{
        when(ses.scheduleAtFixedRate(any(Runnable.class),anyLong(),anyLong(),any(TimeUnit.class))).then(answer);
    }




    @Test
    public void canGetConnectionFromSingleServer() throws Exception{
       PoolSizingStrategy pss = InfinitePoolSize.INSTANCE;

        ConnectionSelectionStrategy css = ConnectionStrategy.ROUND_ROBIN;

        FailureDetectorFactory failureDetectorFactory=()->new DeadlineFailureDetector(Long.MAX_VALUE);
        ServerPoolFactory spf = new ConfiguredServerPoolFactory("test","tu","tp",failureDetectorFactory,pss){
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

        ServerList sl = new ServerList(css,new ServerPool[]{});
        ServerDiscovery discovery =()->Collections.singletonList("testServer:1527");
        ClusteredDataSource cds = new ClusteredDataSource(sl,spf,discovery, ses,false,0L,0);
        cds.detectServers();
//        cds.start();
        Connection c = cds.getConnection();
        Assert.assertNotNull("Did not get a non-null connection!",c);
    }

    @Test
    public void throwsErrorWithNoServers() throws Exception{
        PoolSizingStrategy pss = InfinitePoolSize.INSTANCE;

        ConnectionSelectionStrategy css = ConnectionStrategy.ROUND_ROBIN;

        FailureDetectorFactory failureDetectorFactory=()->{
            //don't have the failure detector advance
            return new DeadlineFailureDetector(10){
                @Override protected long currentTime(){ return 1L; }
            };
        };
        ServerPoolFactory spf = new ConfiguredServerPoolFactory("test","tu","tp",failureDetectorFactory,pss){
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
        ServerDiscovery sd =Collections::emptyList;
        ClusteredDataSource cds = new ClusteredDataSource(new ServerList(css,new ServerPool[]{}),spf,sd, ses,false,0L,0);
        try{
            cds.start();
            cds.getConnection();
            Assert.fail("Did not throw exception!");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect SQLState!", SQLState.NO_CURRENT_CONNECTION,se.getSQLState());
        }
    }

    @Test
    public void throwsErrorWithDerbyConnectionRefused() throws Exception{
        PoolSizingStrategy pss = InfinitePoolSize.INSTANCE;

        ConnectionSelectionStrategy css = ConnectionStrategy.ROUND_ROBIN;

        FailureDetectorFactory failureDetectorFactory=()->{
            //don't have the failure detector advance
            return new DeadlineFailureDetector(10){
                @Override protected long currentTime(){ return 1L; }
            };
        };
        //in this case, we want to use derby's logic so that we know we actually try to connect
        ServerPoolFactory spf = new ConfiguredServerPoolFactory("test","tu","tp",failureDetectorFactory,pss);
        ServerList sl = new ServerList(css,new ServerPool[]{});
        ServerDiscovery sd = new ConnectionServerDiscovery(new String[]{"localhost:60000"},sl,spf);
        ClusteredDataSource cds = new ClusteredDataSource(new ServerList(css,new ServerPool[]{}),spf,sd, ses,false,0L,0);
        cds.detectServers();
        try{
            cds.getConnection();
            Assert.fail("Did not throw exception!");
        }catch(SQLException se){
            se.printStackTrace();
            Assert.assertEquals("Incorrect SQLState!",
                    SQLState.NO_CURRENT_CONNECTION,se.getSQLState());
        }
    }

    @Test
    public void blacklistsServerWithConnectionRefused() throws Exception{
        PoolSizingStrategy pss = InfinitePoolSize.INSTANCE;

        ConnectionSelectionStrategy css =(previous,numServers)->0;

        FailureDetectorFactory failureDetectorFactory=new FailureDetectorFactory(){
            @Override public FailureDetector newFailureDetector(){
                return new DeadlineFailureDetector(Long.MAX_VALUE){
                    @Override
                    public void failed(){
                        kill();
                        super.failed();
                    }
                };
            }
        };
        ServerPoolFactory spf = new ConfiguredServerPoolFactory("test","tu","tp",failureDetectorFactory,pss){
            @Override
            protected DataSource newDataSource(String serverId,
                                               String database,
                                               String user,
                                               String password){
                DataSource ds=mock(DataSource.class);
                if(serverId.equals("failServer")){
                    SQLNonTransientConnectionException e = new SQLNonTransientConnectionException(
                            null,SQLState.AUTH_DATABASE_CONNECTION_REFUSED,new ConnectException("Connection refused"));
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
        ServerList sl = new ServerList(css,new ServerPool[]{});
        ServerDiscovery sd =()->Arrays.asList("failServer","successServer");
        ClusteredDataSource cds = new ClusteredDataSource(sl,spf,sd,ses,false,100L,0);
        cds.detectServers();
        Connection c=cds.getConnection();
        Assert.assertNotNull("Returned a null connection!",c);
        Assert.assertFalse("Returned a closed connection!",c.isClosed());


        Collection<String> blacklistedServers=sl.blacklist();
        Assert.assertEquals("Incorrect # of blacklisted servers",1,blacklistedServers.size());
        Assert.assertTrue("Did not contain correct server!",blacklistedServers.contains("failServer"));
    }

    @Test
    public void serviceDiscoveryAddsServer() throws Exception{
        PoolSizingStrategy pss = InfinitePoolSize.INSTANCE;

        ConnectionSelectionStrategy css = ConnectionStrategy.ROUND_ROBIN;

        FailureDetectorFactory failureDetectorFactory=()->{
            //don't have the failure detector advance
            return new DeadlineFailureDetector(Long.MAX_VALUE);
        };
        ServerPoolFactory spf = new ConfiguredServerPoolFactory("test","tu","tp",failureDetectorFactory,pss){
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
                        @Override public String answer(InvocationOnMock invocation) throws Throwable{ return servers.next(); }
                    });
                    when(rs.getString(2)).thenReturn("1527");
                    when(s.executeQuery(ConnectionServerDiscovery.DEFAULT_ACTIVE_SERVER_QUERY)).thenReturn(rs);
                    when(conn.createStatement()).thenReturn(s);
                    when(ds.getConnection()).thenReturn(conn);
                    when(ds.getConnection(anyString(),anyString())).thenReturn(conn);
                }catch(SQLException se){
                    throw new RuntimeException(se);
                }
                return ds;
            }
        };
        ServerList sl = new ServerList(css,new ServerPool[]{spf.newServerPool("oldServer")});
        ServerDiscovery d = new ConnectionServerDiscovery(new String[]{"oldServer"},sl,spf);

        ClusteredDataSource cds = new ClusteredDataSource(sl,spf,d,ses,false,0,0);
        Set<String> activeServers = sl.liveServers();
        Assert.assertEquals("Incorrect default size!",1,activeServers.size());
        Assert.assertTrue("Did not contain oldServer",activeServers.contains("oldServer"));

        cds.detectServers();
        activeServers = sl.liveServers();
        Assert.assertEquals("Did not add a new server!",2,activeServers.size());
        Assert.assertTrue("Did not contain oldServer!",activeServers.contains("oldServer:1527"));
        Assert.assertTrue("Did not contain newServer!",activeServers.contains("newServer:1527"));
    }

    @Test
    public void serviceDiscoveryRemovesServer() throws Exception{
        PoolSizingStrategy pss =InfinitePoolSize.INSTANCE;

        ConnectionSelectionStrategy css = ConnectionStrategy.ROUND_ROBIN;

        FailureDetectorFactory failureDetectorFactory=()->new DeadlineFailureDetector(Long.MAX_VALUE);
        //in this case, we want to use derby's logic so that we know we actually try to connect
        ServerPoolFactory spf = new ConfiguredServerPoolFactory("test","tu","tp",failureDetectorFactory,pss){
            @Override
            protected DataSource newDataSource(String serverId,
                                               String database,
                                               String user,
                                               String password){
                DataSource ds = mock(DataSource.class);
                if(serverId.equals("deadServer")){
                    try{
                        when(ds.getConnection()).thenThrow(new SQLNonTransientConnectionException(null,SQLState.CONNECT_SOCKET_EXCEPTION,new ConnectException("Connection refused")));
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
                        when(rs.getString(2)).thenReturn("1527");
                        when(s.executeQuery(ConnectionServerDiscovery.DEFAULT_ACTIVE_SERVER_QUERY)).thenReturn(rs);
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
        ServerList sl = new ServerList(css,new ServerPool[]{spf.newServerPool("oldServer"),spf.newServerPool("deadServer")});
        ServerDiscovery sd = new ConnectionServerDiscovery(new String[]{"oldServer","deadServer"},sl,spf);
        ClusteredDataSource cds = new ClusteredDataSource(sl,spf,sd,ses,false,0L,0);
        Set<String> activeServers = sl.liveServers();
        Assert.assertEquals("Incorrect size!",2,activeServers.size());

        cds.detectServers();

        activeServers = sl.liveServers();
        Assert.assertEquals("Did not remove deadServer!",1,activeServers.size());
        Assert.assertTrue("Did not contain oldServer!",activeServers.contains("oldServer:1527"));
        Assert.assertFalse("Still contains deadServer!",activeServers.contains("deadServer"));

        //get a few connections just to make sure that we don't ever call newDataSource
        for(int i=0;i<5;i++){
            Connection conn = cds.getConnection();
            conn.close();
        }
    }
}