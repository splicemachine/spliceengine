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
import com.splicemachine.db.jdbc.ClientDriver40;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.sql.DataSource;
import java.net.ConnectException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 *         Date: 8/16/16
 */
public class ServerPoolTest{
    private static final FailureDetector noFailDetector=new FailureDetector(){
        @Override public void success(){ }
        @Override public boolean failed(){ return false; }
        @Override public boolean isAlive(){ return true; }
        @Override public void kill(){ }
    };
    private static final PoolSizingStrategy poolSizingStrategy = new PoolSizingStrategy(){
        @Override public void acquirePermit(){ }
        @Override public void releasePermit(){ }
        @Override public int singleServerPoolSize(){ return 1; }
    };

    private static final BlackList<ServerPool> blackList = new BlackList<ServerPool>(){
        @Override
        protected void cleanupResources(ServerPool element){
            try{ element.close(); }catch(SQLException e){ throw new RuntimeException(e); }
        }
    };

    @Test
    public void blah() throws Exception{
        DriverManager.setLoginTimeout(1);
        Connection conn = new ClientDriver40().connect("jdbc:splice://10.1.1.237:1527/splicedb;user=splice;password=admin",null);

        System.out.println(conn.isClosed());
        long s = System.nanoTime();
        System.out.println(conn.isValid(1));
        System.out.printf("time taken: %f%n",(System.nanoTime()-s)/1000d/1000d);
    }

    @Test
    public void tryAcquireConnectionWorks() throws Exception{
        Connection conn = mock(Connection.class);
        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(conn);
        when(ds.getConnection(anyString(),anyString())).thenReturn(conn);

        ServerPool sp = new ServerPool(ds,"testServer",10,
                new DeadlineFailureDetector(Long.MAX_VALUE),poolSizingStrategy,blackList,10);

        Connection c = sp.tryAcquireConnection();
        Assert.assertNotNull("Did not return a connection!",c);
    }

    @Test
    public void tryAcquireConnectionReturnsEmptyIfPoolIsFull() throws Exception{
        Connection conn = mock(Connection.class);
        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(conn);
        when(ds.getConnection(anyString(),anyString())).thenReturn(conn);

        ServerPool sp = new ServerPool(ds,"testServer",1,
                noFailDetector,poolSizingStrategy,blackList,10);

        Connection c = sp.tryAcquireConnection();
        Assert.assertNotNull("Did not return a connection!",c);

        Connection shouldBeNull = sp.tryAcquireConnection();
        Assert.assertNull("Returned too many connections!",shouldBeNull);
    }

    @Test
    public void tryAcquireConnectionFailsIfInvalidUsername() throws Exception{
        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenThrow(new SQLNonTransientConnectionException(null,SQLState.LOGIN_FAILED));

        ServerPool sp = new ServerPool(ds,"testServer",1,noFailDetector,poolSizingStrategy,blackList,10);

        try{
            sp.tryAcquireConnection();
            Assert.fail("Did not fail login error");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error code!",SQLState.LOGIN_FAILED,se.getSQLState());
        }
    }

    @Test
    public void tryAcquireConnectionRetriesOnConnectionRefused() throws Exception{
        final Connection conn = mock(Connection.class);
        DataSource ds = mock(DataSource.class);
        final boolean[] visited = new boolean[]{false};
        when(ds.getConnection()).then(new Answer<Connection>(){
            @Override
            public Connection answer(InvocationOnMock invocation) throws Throwable{
                if(!visited[0]){
                    visited[0] = true;
                    throw new SQLNonTransientConnectionException(null,"08001",new ConnectException("Connection refused"));
                }else return conn;
            }
        });

        ServerPool sp = new ServerPool(ds,"testServer",1,noFailDetector,poolSizingStrategy,blackList,10);

        Connection pooledConn= sp.tryAcquireConnection();
        Assert.assertNotNull("Did not return a connection!",pooledConn);
    }

    /*close tests*/
    @Test
    public void closeWorksWhenAllConnectionsAreClosed() throws Exception{
        Connection conn = mock(Connection.class);
        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(conn);
        when(ds.getConnection(anyString(),anyString())).thenReturn(conn);

        ServerPool sp = new ServerPool(ds,"testServer",1,
                new DeadlineFailureDetector(Long.MAX_VALUE),poolSizingStrategy,blackList,10);

        Connection c = sp.tryAcquireConnection();
        Assert.assertNotNull("Did not return a connection!",c);

        c.close();

        sp.close();
    }

    @Test
    public void closeThrowsErrorsIfNotAllConnectionsAreReleased() throws Exception{
        Connection conn = mock(Connection.class);
        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(conn);
        when(ds.getConnection(anyString(),anyString())).thenReturn(conn);

        ServerPool sp = new ServerPool(ds,"testServer",1,
                new DeadlineFailureDetector(Long.MAX_VALUE),poolSizingStrategy,blackList,10);

        Connection c = sp.tryAcquireConnection();
        Assert.assertNotNull("Did not return a connection!",c);

        try{
            sp.close();
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error code!",SQLState.CANNOT_CLOSE_ACTIVE_CONNECTION,se.getSQLState());
        }
    }
}