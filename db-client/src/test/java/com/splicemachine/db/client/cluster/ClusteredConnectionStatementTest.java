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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 *         Date: 11/15/16
 */
@SuppressWarnings("MagicConstant")
public class ClusteredConnectionStatementTest{

    @Test
    public void testCanAcquireStatement() throws Exception{
        /*
         * Test of the common statement access pattern
         */
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection baseConn = mock(Connection.class);
        Statement mockS = mock(Statement.class);
        boolean[] called = new boolean[]{false};
        when(mockS.execute(anyString())).thenAnswer(new Answer<Boolean>(){
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable{
                Assert.assertFalse("Query executed twice!",called[0]);
                called[0] = true;
                return true;
            }
        });
        when(baseConn.createStatement()).thenReturn(mockS);
        when(baseConn.createStatement(anyInt(),anyInt(),anyInt())).thenReturn(mockS);
        when(cds.getConnection()).thenReturn(baseConn);

        ClusteredConnection conn = new ClusteredConnection("testUrl",
                cds,
                true,
                new Properties());

        Statement s = conn.createStatement();
        s.execute("values (1)");

        s.close();
        conn.close();

    }

    @Test
    public void testCanAcquireStatementAutoCommitOff() throws Exception{
        /*
         * Test of the common statement access pattern
         */
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection baseConn = mock(Connection.class);
        Statement mockS = mock(Statement.class);
        boolean[] called = new boolean[]{false};
        when(mockS.execute(anyString())).thenAnswer(new Answer<Boolean>(){
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable{
                Assert.assertFalse("Query executed twice!",called[0]);
                called[0] = true;
                return true;
            }
        });
        when(baseConn.createStatement()).thenReturn(mockS);
        when(baseConn.createStatement(anyInt(),anyInt(),anyInt())).thenReturn(mockS);
        when(cds.getConnection()).thenReturn(baseConn);

        ClusteredConnection conn = new ClusteredConnection("testUrl",
                cds,
                true,
                new Properties());
        conn.setAutoCommit(false);

        Statement s = conn.createStatement();
        s.execute("values (1)");

        s.close();
        conn.close();
    }

    @Test
    public void testStatementWorksWithTransientNetworkFailure() throws Exception{
        /*
         * Tests that a transient network error is transparently retried, if
         * auto commit is on.
         */
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection baseConn = mock(Connection.class);
        Statement mockS = mock(Statement.class);
        int[] called = new int[]{0};
        when(mockS.execute(anyString())).thenAnswer(new Answer<Boolean>(){
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable{
                switch(called[0]){
                    case 0:
                        called[0]++;
                        throw new SQLException("Connection reset",SQLState.SOCKET_EXCEPTION);
                    case 1:
                        called[0]++;
                        return true;
                    default:
                        Assert.fail("Query executed too many times!");
                        return false;
                }
            }
        });
        when(baseConn.createStatement()).thenReturn(mockS);
        when(baseConn.createStatement(anyInt(),anyInt(),anyInt())).thenReturn(mockS);
        when(cds.getConnection()).thenReturn(baseConn);

        ClusteredConnection conn = new ClusteredConnection("testUrl",
                cds,
                true,
                new Properties());

        Statement s = conn.createStatement();
        s.execute("values (1)");

        s.close();
        conn.close();
        Assert.assertEquals("Did not retry properly!",2,called[0]);
    }

    @Test
    public void testStatementDoesNotHideTransientErrorsWithAutoCommitOff() throws Exception{
        /*
         * Tests that a transient network error is NOT retried if auto-commit is off. However,
         * the connection itself remains valid and can be re-executed.
         */
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection baseConn = mock(Connection.class);
        Statement mockS = mock(Statement.class);
        int[] called = new int[]{0};
        when(mockS.execute(anyString())).thenAnswer(new Answer<Boolean>(){
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable{
                switch(called[0]){
                    case 0:
                        called[0]++;
                        throw new SQLException("Connection reset",SQLState.SOCKET_EXCEPTION);
                    case 1:
                        called[0]++;
                        return true;
                    default:
                        Assert.fail("Query executed too many times!");
                        return false;
                }
            }
        });
        when(baseConn.createStatement()).thenReturn(mockS);
        when(baseConn.createStatement(anyInt(),anyInt(),anyInt())).thenReturn(mockS);
        when(cds.getConnection()).thenReturn(baseConn);

        ClusteredConnection conn = new ClusteredConnection("testUrl",
                cds,
                true,
                new Properties());
        conn.setAutoCommit(false);

        Statement s = conn.createStatement();
        try{
            s.execute("values (1)");
            Assert.fail("Did not catch an exception!");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect SQL state!",SQLState.SOCKET_EXCEPTION,se.getSQLState());
        }

        s.execute("values (1)");

        s.close();
        conn.close();
        Assert.assertEquals("Did not call executions properly!",2,called[0]);
    }

    @Test
    public void testStatementDoesNotHideNonTransientErrors() throws Exception{
        /*
         * Tests that a transient network error is transparently retried, if
         * auto commit is on.
         */
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection baseConn = mock(Connection.class);
        Statement mockS = mock(Statement.class);
        int[] called = new int[]{0};
        when(mockS.execute(anyString())).thenAnswer(new Answer<Boolean>(){
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable{
                switch(called[0]){
                    case 0:
                        called[0]++;
                        throw new SQLException("Write Conflict","SE014");
                    case 1:
                        called[0]++;
                        return true;
                    default:
                        Assert.fail("Query executed too many times!");
                        return false;
                }
            }
        });
        when(baseConn.createStatement()).thenReturn(mockS);
        when(baseConn.createStatement(anyInt(),anyInt(),anyInt())).thenReturn(mockS);
        when(cds.getConnection()).thenReturn(baseConn);

        ClusteredConnection conn = new ClusteredConnection("testUrl",
                cds,
                true,
                new Properties());

        Statement s = conn.createStatement();
        try{
            s.execute("values (1)");
            Assert.fail("Did not catch an exception!");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect SQL state!","SE014",se.getSQLState());
        }

        s.close();
        conn.close();
        Assert.assertEquals("Retried incorrectly!",1,called[0]);
    }
}