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

import com.splicemachine.db.shared.common.reference.SQLState;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.sql.DataSource;
import java.sql.*;
import java.util.LinkedList;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @author Scott Fines
 *         Date: 8/16/16
 */
public class ServerPoolTest{
    private static final FailureDetector noFailDetector=new FailureDetector(){
        @Override public void success(){ }
        @Override public void failed(){ }
        @Override public boolean isAlive(){ return true; }
        @Override public void kill(){ }
        @Override public double failureProbability(){ return 0; }
    };
    private static final PoolSizingStrategy poolSizingStrategy = new PoolSizingStrategy(){
        @Override public void acquirePermit(){ }
        @Override public void releasePermit(){ }
        @Override public int singleServerPoolSize(){ return 1; }
    };


    /*re-using pool tests*/

    @Test
    public void connectionCanBePulledFromPool() throws Exception{
        final Connection conn = mock(Connection.class);
        when(conn.isValid(anyInt())).thenReturn(true);

        DataSource ds = mock(DataSource.class);
        final boolean[] visited = new boolean[]{false};
        when(ds.getConnection()).then(new Answer<Connection>(){
            @Override
            public Connection answer(InvocationOnMock invocation) throws Throwable{
                Assert.assertFalse("Attempted to create more than one connection!",visited[0]);
                visited[0] = true;
                return conn;
            }
        });

        ServerPool sp = new ServerPool(ds,"testServer",10, noFailDetector,poolSizingStrategy,10);

        Connection c = sp.tryAcquireConnection(true);
        Assert.assertNotNull("Did not return a connection!",c);
        c.close(); //return to pool

        //now try and get it out of the pool again
        c = sp.tryAcquireConnection(true);
        Assert.assertNotNull("Did not return a connection!",c);
    }

    @Test
    public void poolDoesNotReturnAClosedConnection() throws Exception{
        Connection conn = mock(Connection.class);
        when(conn.isValid(anyInt())).thenReturn(true);
        when(conn.isClosed()).thenReturn(false);

        DataSource ds = mock(DataSource.class);
        final LinkedList<Connection> conns = new LinkedList<>();
        conns.add(conn);

        Connection conn2 = mock(Connection.class);
        when(conn2.isValid(anyInt())).thenReturn(true);
        when(conn2.isClosed()).thenReturn(false);
        conns.add(conn2);

        when(ds.getConnection()).then(new Answer<Connection>(){
            @Override
            public Connection answer(InvocationOnMock invocation) throws Throwable{
                Connection c = conns.poll();
                Assert.assertNotNull("Created a new connection too many times!");
                return c;
            }
        });

        ServerPool sp = new ServerPool(ds,"testServer",10, noFailDetector,poolSizingStrategy,10);

        Connection c = sp.tryAcquireConnection(true);
        Assert.assertNotNull("Did not return a connection!",c);
        c.close(); //return to pool

        //now close the underlying pool
        when(conn.isClosed()).thenReturn(true);

        //now try and get it out of the pool again
        c = sp.tryAcquireConnection(true);
        Assert.assertNotNull("Did not return a connection!",c);
        Assert.assertFalse("Returned a closed connection!",c.isClosed());
    }

    @Test
    public void poolDoesNotReturnAnInvalidConnection() throws Exception{
        Connection conn = mock(Connection.class);
        when(conn.isValid(anyInt())).thenReturn(true);
        when(conn.isClosed()).thenReturn(false);

        DataSource ds = mock(DataSource.class);
        final LinkedList<Connection> conns = new LinkedList<>();
        conns.add(conn);

        Connection conn2 = mock(Connection.class);
        when(conn2.isValid(anyInt())).thenReturn(true);
        when(conn2.isClosed()).thenReturn(false);
        conns.add(conn2);

        when(ds.getConnection()).then(new Answer<Connection>(){
            @Override
            public Connection answer(InvocationOnMock invocation) throws Throwable{
                Connection c = conns.poll();
                Assert.assertNotNull("Created a new connection too many times!");
                return c;
            }
        });

        ServerPool sp = new ServerPool(ds,"testServer",10, noFailDetector,poolSizingStrategy,10);

        Connection c = sp.tryAcquireConnection(true);
        Assert.assertNotNull("Did not return a connection!",c);
        c.close(); //return to pool

        //now close the underlying pool
        when(conn.isValid(anyInt())).thenReturn(false);

        //now try and get it out of the pool again
        c = sp.tryAcquireConnection(true);
        Assert.assertNotNull("Did not return a connection!",c);
        Assert.assertFalse("Returned a closed connection!",c.isClosed());
        Assert.assertTrue("Returned an invalid connection!",c.isValid(10));
    }

    @Test
    public void repeatedConnectionErrorsEventuallyFail() throws Exception{
        DataSource ds =  mock(DataSource.class);
        when(ds.getConnection()).then(new Answer<Connection>(){
            @Override
            public Connection answer(InvocationOnMock invocation) throws Throwable{
                throw new SQLNonTransientConnectionException("Connection error",SQLState.CONNECT_SOCKET_EXCEPTION);
            }
        });

        ServerPool sp = new ServerPool(ds,"testServer",10, noFailDetector,poolSizingStrategy,10);
        try{
            sp.tryAcquireConnection(true);
            Assert.fail("Did not throw an error");
        }catch(SQLNonTransientConnectionException se){
            //specifically catch only Non-transient errors
            Assert.assertEquals("Incorrect error message thrown",SQLState.CONNECT_SOCKET_EXCEPTION,se.getSQLState());
        }
    }

    /*tryAcquire->create new connection tests*/
    @Test
    public void tryAcquireConnectionWorks() throws Exception{
        Connection conn = mock(Connection.class);
        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(conn);
        when(ds.getConnection(anyString(),anyString())).thenReturn(conn);

        ServerPool sp = new ServerPool(ds,"testServer",10,
                new DeadlineFailureDetector(Long.MAX_VALUE),poolSizingStrategy,10);

        Connection c = sp.tryAcquireConnection(true);
        Assert.assertNotNull("Did not return a connection!",c);
    }

    @Test
    public void tryAcquireConnectionReturnsEmptyIfPoolIsFull() throws Exception{
        Connection conn = mock(Connection.class);
        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(conn);
        when(ds.getConnection(anyString(),anyString())).thenReturn(conn);

        ServerPool sp = new ServerPool(ds,"testServer",1,
                noFailDetector,poolSizingStrategy,10);

        Connection c = sp.tryAcquireConnection(true);
        Assert.assertNotNull("Did not return a connection!",c);

        Connection shouldBeNull = sp.tryAcquireConnection(true);
        Assert.assertNull("Returned too many connections!",shouldBeNull);
    }

    @Test
    public void tryAcquireConnectionFailsIfInvalidUsername() throws Exception{
        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenThrow(new SQLNonTransientConnectionException(null,SQLState.LOGIN_FAILED));

        ServerPool sp = new ServerPool(ds,"testServer",1,noFailDetector,poolSizingStrategy,10);

        try{
            sp.tryAcquireConnection(true);
            Assert.fail("Did not fail login error");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error code!",SQLState.LOGIN_FAILED,se.getSQLState());
        }
    }

    /*heartbeat tests*/
    @Test
    public void heartbeatFunctionsOnSuccessfulConnection() throws Exception{
        Connection conn = mock(Connection.class);
        when(conn.isValid(anyInt())).thenReturn(true); //this is a valid connection
        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(conn);

        FailureDetector tfd = new FailureDetector(){
            private boolean sawSuccess = false;
            @Override public void kill(){ }
            @Override
            public void success(){
                sawSuccess = true;
            }

            @Override public double failureProbability(){ return isAlive()? 0: 1; }
            @Override
            public void failed(){
                Assert.fail("Should not have marked failed!");
            }

            @Override
            public boolean isAlive(){
                return sawSuccess;
            }
        };

        ServerPool sp = new ServerPool(ds,"testServer",1,tfd,poolSizingStrategy,10);

        sp.heartbeat();

        Assert.assertTrue("Failure Detector did not see success!",tfd.isAlive());
        Assert.assertFalse("Server is not treated as alive!",sp.isDead());
    }

    @Test
    public void heartbeatFailsOnUnsuccessfulConnection() throws Exception{
        Connection conn = mock(Connection.class);
        when(conn.isValid(anyInt())).thenReturn(false); //this is a valid connection
        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(conn);

        FailureDetector tfd = new FailureDetector(){
            private boolean sawFailure=false;
            @Override public void kill(){ }
            @Override
            public void success(){
                Assert.fail("Should not see success!");
            }

            @Override public double failureProbability(){ return isAlive()? 0: 1; }
            @Override
            public void failed(){
                sawFailure = true;
            }

            @Override
            public boolean isAlive(){
                return !sawFailure;
            }
        };

        ServerPool sp = new ServerPool(ds,"testServer",1,tfd,poolSizingStrategy,10);

        sp.heartbeat();

        Assert.assertFalse("Failure Detector did not see failure!",tfd.isAlive());
        Assert.assertTrue("Server is not dead!",sp.isDead());
    }

    @Test
    public void heartbeatFailsOnConnectionError() throws Exception{
        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).then(new Answer<Connection>(){
            @Override
            public Connection answer(InvocationOnMock invocation) throws Throwable{
                throw new SQLNonTransientConnectionException("connection refused",SQLState.CONNECT_SOCKET_EXCEPTION);
            }
        });

        FailureDetector tfd = new FailureDetector(){
            private boolean sawFailure=false;
            @Override public void kill(){ }
            @Override
            public void success(){
                Assert.fail("Should not see success!");
            }

            @Override public double failureProbability(){ return isAlive()? 0: 1; }
            @Override
            public void failed(){
                sawFailure = true;
            }

            @Override
            public boolean isAlive(){
                return !sawFailure;
            }
        };

        ServerPool sp = new ServerPool(ds,"testServer",1,tfd,poolSizingStrategy,10);

        sp.heartbeat();

        Assert.assertFalse("Failure Detector did not see failure!",tfd.isAlive());
        Assert.assertTrue("Server is not dead!",sp.isDead());
    }

    /*close tests*/
    @Test
    public void closeWorksWhenAllConnectionsAreClosed() throws Exception{
        Connection conn = mock(Connection.class);
        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(conn);
        when(ds.getConnection(anyString(),anyString())).thenReturn(conn);

        ServerPool sp = new ServerPool(ds,"testServer",1,
                new DeadlineFailureDetector(Long.MAX_VALUE),poolSizingStrategy,10);

        Connection c = sp.tryAcquireConnection(true);
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
                new DeadlineFailureDetector(Long.MAX_VALUE),poolSizingStrategy,10);

        Connection c = sp.tryAcquireConnection(true);
        Assert.assertNotNull("Did not return a connection!",c);

        try{
            sp.close();
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error code!",SQLState.CANNOT_CLOSE_ACTIVE_CONNECTION,se.getSQLState());
        }
    }

    @Test
    public void closeThrowsErrorIfConnectionCloseThrowsError() throws Exception{
        Connection errorConn = mock(Connection.class);

        Connection goodConn = mock(Connection.class);

        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(goodConn,errorConn);

        ServerPool sp = new ServerPool(ds,"testServer",10, noFailDetector,poolSizingStrategy,10);

        Connection c = sp.tryAcquireConnection(true);
        Assert.assertNotNull("Did not return a connection!",c);
        Connection c2 = sp.tryAcquireConnection(true);
        Assert.assertNotNull("Did not return a connection!",c2);
        c.close();
        c2.close();

        //make sure that the close() method goes bad
        doThrow(new SQLNonTransientConnectionException("Something bad happened",SQLState.PHYSICAL_CONNECTION_ALREADY_CLOSED)).when(errorConn).close();
        try{
            sp.close();
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error code!",SQLState.PHYSICAL_CONNECTION_ALREADY_CLOSED,se.getSQLState());
        }
    }

    /*Statement tests*/

    @Test
    public void statementRecordsNetworkFailureToFailureDetector() throws Exception{
        Connection baseConn = mock(Connection.class);
        Statement errorStatement = mock(Statement.class);
        when(errorStatement.executeQuery(anyString())).thenThrow(new SQLNonTransientConnectionException("Connection Refused",SQLState.CONNECT_SOCKET_EXCEPTION));
        when(baseConn.createStatement()).thenReturn(errorStatement);

        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(baseConn);

        FailureDetector fd=failOnlyDetector();
        ServerPool sp = new ServerPool(ds,"testServer",1,fd,poolSizingStrategy,10);

        Connection connection=sp.tryAcquireConnection(true);
        Assert.assertNotNull("Did not return a connection!",connection);
        try(Statement s = connection.createStatement()){
            s.executeQuery("values (1)");
            Assert.fail("Did not receive an exception");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error code!",SQLState.CONNECT_SOCKET_EXCEPTION,se.getSQLState());
        }

        Assert.assertFalse("did not receive failure notice!",fd.isAlive());
    }

    @Test
    public void statementIgnoresNonNetworkError() throws Exception{
        Connection baseConn = mock(Connection.class);
        Statement errorStatement = mock(Statement.class);
        when(errorStatement.executeQuery(anyString())).thenThrow(new SQLException("PK Violation",SQLState.LANG_DUPLICATE_KEY_CONSTRAINT));
        when(baseConn.createStatement()).thenReturn(errorStatement);

        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(baseConn);

        FailureDetector fd=new FailureDetector(){
            @Override public void success(){ }

            @Override
            public void failed(){
                Assert.fail("Should not have called failed())");
            }

            @Override public double failureProbability(){ return 0; }
            @Override public boolean isAlive(){ return true; }
            @Override public void kill(){ }
        };
        ServerPool sp = new ServerPool(ds,"testServer",1,fd,poolSizingStrategy,10);

        Connection connection=sp.tryAcquireConnection(true);
        Assert.assertNotNull("Did not return a connection!",connection);
        try(Statement s = connection.createStatement()){
            s.executeQuery("values (1)");
            Assert.fail("Did not receive an exception");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error code!",SQLState.LANG_DUPLICATE_KEY_CONSTRAINT,se.getSQLState());
        }
    }

    @Test
    public void preparedStatementRecordsNetworkFailureToFailureDetector() throws Exception{
        Connection baseConn = mock(Connection.class);
        PreparedStatement errorStatement = mock(PreparedStatement.class);
        when(errorStatement.executeQuery()).thenThrow(new SQLNonTransientConnectionException("Connection Refused",SQLState.CONNECT_SOCKET_EXCEPTION));
        when(baseConn.prepareStatement(anyString())).thenReturn(errorStatement);

        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(baseConn);

        FailureDetector fd=failOnlyDetector();
        ServerPool sp = new ServerPool(ds,"testServer",1,fd,poolSizingStrategy,10);

        Connection connection=sp.tryAcquireConnection(true);
        Assert.assertNotNull("Did not return a connection!",connection);
        try(PreparedStatement s = connection.prepareStatement("values (1)")){
            s.executeQuery();
            Assert.fail("Did not receive an exception");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error code!",SQLState.CONNECT_SOCKET_EXCEPTION,se.getSQLState());
        }

        Assert.assertFalse("did not receive failure notice!",fd.isAlive());
    }

    @Test
    public void preparedStatementIgnoresNonNetworkError() throws Exception{
        Connection baseConn = mock(Connection.class);
        PreparedStatement errorStatement = mock(PreparedStatement.class);
        when(errorStatement.executeQuery()).thenThrow(new SQLException("PK Violation",SQLState.LANG_DUPLICATE_KEY_CONSTRAINT));
        when(baseConn.prepareStatement(anyString())).thenReturn(errorStatement);

        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(baseConn);

        FailureDetector fd=new FailureDetector(){
            @Override public void success(){ }

            @Override public double failureProbability(){ return 0; }
            @Override
            public void failed(){
                Assert.fail("Should not have called failed())");
            }

            @Override public boolean isAlive(){ return true; }

            @Override public void kill(){ }
        };
        ServerPool sp = new ServerPool(ds,"testServer",1,fd,poolSizingStrategy,10);

        Connection connection=sp.tryAcquireConnection(true);
        Assert.assertNotNull("Did not return a connection!",connection);
        try(PreparedStatement s = connection.prepareStatement("values (1)")){
            s.executeQuery();
            Assert.fail("Did not receive an exception");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error code!",SQLState.LANG_DUPLICATE_KEY_CONSTRAINT,se.getSQLState());
        }
    }

    @Test
    public void callableStatementRecordsNetworkFailureToFailureDetector() throws Exception{
        Connection baseConn = mock(Connection.class);
        CallableStatement errorStatement = mock(CallableStatement.class);
        when(errorStatement.executeQuery()).thenThrow(new SQLNonTransientConnectionException("Connection Refused",SQLState.CONNECT_SOCKET_EXCEPTION));
        when(baseConn.prepareCall(anyString())).thenReturn(errorStatement);

        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(baseConn);

        FailureDetector fd=failOnlyDetector();
        ServerPool sp = new ServerPool(ds,"testServer",1,fd,poolSizingStrategy,10);

        Connection connection=sp.tryAcquireConnection(true);
        Assert.assertNotNull("Did not return a connection!",connection);
        try(CallableStatement s = connection.prepareCall("call SYSCS_UTIL.SYSCS_GET_ACTIVE_SERVERS()")){
            s.executeQuery();
            Assert.fail("Did not receive an exception");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error code!",SQLState.CONNECT_SOCKET_EXCEPTION,se.getSQLState());
        }

        Assert.assertFalse("did not receive failure notice!",fd.isAlive());
    }

    @Test
    public void callableStatementIgnoresNonNetworkError() throws Exception{
        Connection baseConn = mock(Connection.class);
        CallableStatement errorStatement = mock(CallableStatement.class);
        when(errorStatement.executeQuery()).thenThrow(new SQLException("PK Violation",SQLState.LANG_DUPLICATE_KEY_CONSTRAINT));
        when(baseConn.prepareCall(anyString())).thenReturn(errorStatement);

        DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(baseConn);

        FailureDetector fd=new FailureDetector(){
            @Override public void success(){ }

            @Override
            public void failed(){
                Assert.fail("Should not have called failed())");
            }
            @Override public double failureProbability(){ return 0; }
            @Override public boolean isAlive(){ return true; }
            @Override public void kill(){ }
        };
        ServerPool sp = new ServerPool(ds,"testServer",1,fd,poolSizingStrategy,10);

        Connection connection=sp.tryAcquireConnection(true);
        Assert.assertNotNull("Did not return a connection!",connection);
        try(CallableStatement s = connection.prepareCall("call SYSCS_UTIL.SYSCS_GET_ACTIVE_SERVERS()")){
            s.executeQuery();
            Assert.fail("Did not receive an exception");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error code!",SQLState.LANG_DUPLICATE_KEY_CONSTRAINT,se.getSQLState());
        }
    }

    private FailureDetector failOnlyDetector(){
        return new FailureDetector(){
            private boolean gotFailure = false;
            @Override public void success(){ Assert.fail("Should not record success"); }
            @Override public void kill(){ Assert.fail("Should not be calling kill"); }

            @Override public double failureProbability(){ return isAlive()? 0: 1; }

            @Override
            public void failed(){
                Assert.assertFalse("Received more than one failure!",gotFailure);
                gotFailure = true;
            }

            @Override
            public boolean isAlive(){
                return !gotFailure;
            }
        };
    }
}