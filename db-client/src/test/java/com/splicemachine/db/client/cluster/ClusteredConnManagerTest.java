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

import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @author Scott Fines
 *         Date: 11/15/16
 */
public class ClusteredConnManagerTest{

    @Test
    public void testSetAutoCommitWithActiveConn() throws Exception{
        /*
         * Makes sure that setting auto-commit will be reflected in an active
         * connection.
         */
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        boolean[] acSet = new boolean[]{true};
        when(conn.getAutoCommit()).thenAnswer(new Answer<Boolean>(){
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable{
                return acSet[0];
            }
        });
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                acSet[0] = (Boolean)invocation.getArguments()[0];
                return null;
            }
        }).when(conn).setAutoCommit(anyBoolean());
        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);

        RefCountedConnection rcc = connManager.acquireConnection();
        Assert.assertTrue("Does not default to auto-commit = true!",rcc.element().getAutoCommit());
        connManager.setAutoCommit(false);
        Assert.assertFalse("Still in auto-commit mode!",rcc.element().getAutoCommit());
    }

    @Test
    public void testSetAutoCommitWithInActiveConn() throws Exception{
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        boolean[] acSet = new boolean[]{true};
        when(conn.getAutoCommit()).thenAnswer(new Answer<Boolean>(){
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable{
                return acSet[0];
            }
        });
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                acSet[0] = (Boolean)invocation.getArguments()[0];
                return null;
            }
        }).when(conn).setAutoCommit(anyBoolean());
        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);

        connManager.setAutoCommit(false); //set before acquiring
        RefCountedConnection rcc = connManager.acquireConnection();
        Assert.assertFalse("Still in auto-commit mode!",rcc.element().getAutoCommit());
    }

    @Test
    public void testSetReadOnlyWithActiveConn() throws Exception{
        /*
         * Makes sure that setting read-only will be reflected in an active
         * connection.
         */
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        boolean[] acSet = new boolean[]{true};
        when(conn.isReadOnly()).thenAnswer(new Answer<Boolean>(){
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable{
                return acSet[0];
            }
        });
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                acSet[0] = (Boolean)invocation.getArguments()[0];
                return null;
            }
        }).when(conn).setReadOnly(anyBoolean());
        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);

        RefCountedConnection rcc = connManager.acquireConnection();
        Assert.assertFalse("Does not default to readOnly = false!",rcc.element().isReadOnly());
        connManager.setReadOnly(true);
        Assert.assertTrue("Still in read-only mode!",rcc.element().isReadOnly());
    }

    @Test
    public void testSetReadOnlyWithInActiveConn() throws Exception{
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        boolean[] acSet = new boolean[]{true};
        when(conn.isReadOnly()).thenAnswer(new Answer<Boolean>(){
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable{
                return acSet[0];
            }
        });
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                acSet[0] = (Boolean)invocation.getArguments()[0];
                return null;
            }
        }).when(conn).setReadOnly(anyBoolean());
        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);

        connManager.setReadOnly(true); //set before acquiring
        RefCountedConnection rcc = connManager.acquireConnection();
        Assert.assertTrue("Still in not-read-only mode!",rcc.element().isReadOnly());
    }

    @Test
    public void testSetHoldabilityWithActiveConn() throws Exception{
        /*
         * Makes sure that setting holdability will be reflected in an active
         * connection.
         */
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        int[] acSet = new int[]{ResultSet.HOLD_CURSORS_OVER_COMMIT};
        when(conn.getHoldability()).thenAnswer(new Answer<Integer>(){
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable{
                return acSet[0];
            }
        });
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                acSet[0] = (Integer)invocation.getArguments()[0];
                return null;
            }
        }).when(conn).setHoldability(anyInt());
        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);

        RefCountedConnection rcc = connManager.acquireConnection();
        Assert.assertEquals("Does not default to HOLD_CURSORS_OVER_COMMIT!",
                ResultSet.HOLD_CURSORS_OVER_COMMIT,rcc.element().getHoldability());
        connManager.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        Assert.assertEquals("Did not set holdability properly!",
                ResultSet.CLOSE_CURSORS_AT_COMMIT,rcc.element().getHoldability());
    }

    @Test
    public void testSetHoldabilityWithInActiveConn() throws Exception{
        /*
         * Makes sure that setting holdability will be reflected in newly created connections
         */
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        int[] acSet = new int[]{ResultSet.HOLD_CURSORS_OVER_COMMIT};
        when(conn.getHoldability()).thenAnswer(new Answer<Integer>(){
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable{
                return acSet[0];
            }
        });
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                acSet[0] = (Integer)invocation.getArguments()[0];
                return null;
            }
        }).when(conn).setHoldability(anyInt());
        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);

        connManager.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        RefCountedConnection rcc = connManager.acquireConnection();
        Assert.assertEquals("Did not set holdability properly!",
                ResultSet.CLOSE_CURSORS_AT_COMMIT, rcc.element().getHoldability());
    }

    @Test
    public void testSetSchemaWithActiveConn() throws Exception{
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        String[] schema = new String[]{"SPLICE"};
        when(conn.getSchema()).then(new Answer<String>(){
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable{
                return schema[0];
            }
        });
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                schema[0] = (String)invocation.getArguments()[0];
                return null;
            }
        }).when(conn).setSchema(anyString());

        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);
        RefCountedConnection rcc=connManager.acquireConnection();
        Assert.assertEquals("Incorrect default schema","SPLICE",rcc.element().getSchema());
        connManager.setSchema("TEST");
        Assert.assertEquals("Did not propagate schema changes!","TEST",rcc.element().getSchema());
    }

    @Test
    public void testSetSchemaWithInActiveConn() throws Exception{
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        String[] schema = new String[]{"SPLICE"};
        when(conn.getSchema()).then(new Answer<String>(){
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable{
                return schema[0];
            }
        });
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                schema[0] = (String)invocation.getArguments()[0];
                return null;
            }
        }).when(conn).setSchema(anyString());

        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);
        connManager.setSchema("TEST");
        RefCountedConnection rcc=connManager.acquireConnection();
        Assert.assertEquals("Did not propagate schema changes!","TEST",rcc.element().getSchema());
    }

    @Test
    public void testGetSchemaWillPullSchemaFromConnectionIfNecessary() throws Exception{
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        String[] schema = new String[]{"SPLICE"};
        when(conn.getSchema()).then(new Answer<String>(){
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable{
                return schema[0];
            }
        });
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                schema[0] = (String)invocation.getArguments()[0];
                return null;
            }
        }).when(conn).setSchema(anyString());

        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);
        Assert.assertEquals("Does not pull schema value from connection!","SPLICE",connManager.getSchema());
    }

    @Test
    @SuppressWarnings("MagicConstant")
    public void testSetIsolationLevelPropagatesToActiveConnections() throws Exception{
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        int[] isolationLevels = new int[]{TxnIsolation.READ_COMMITTED.level};
        when(conn.getTransactionIsolation()).then(new Answer<Integer>(){
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable{
                return isolationLevels[0];
            }
        });
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                isolationLevels[0] = (Integer)invocation.getArguments()[0];
                return null;
            }
        }).when(conn).setTransactionIsolation(anyInt());

        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);
        RefCountedConnection rcc=connManager.acquireConnection();
        Assert.assertEquals("Incorrect default isolation",
                TxnIsolation.READ_COMMITTED.level,rcc.element().getTransactionIsolation());
        connManager.setIsolationLevel(TxnIsolation.SERIALIZABLE);
        Assert.assertEquals("Incorrect set isolation",
                TxnIsolation.SERIALIZABLE.level,rcc.element().getTransactionIsolation());
    }

    @Test
    @SuppressWarnings("MagicConstant")
    public void testSetIsolationLevelPropagatesToNewConnections() throws Exception{
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        int[] isolationLevels = new int[]{TxnIsolation.READ_COMMITTED.level};
        when(conn.getTransactionIsolation()).then(new Answer<Integer>(){
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable{
                return isolationLevels[0];
            }
        });
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                isolationLevels[0] = (Integer)invocation.getArguments()[0];
                return null;
            }
        }).when(conn).setTransactionIsolation(anyInt());

        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);
        connManager.setIsolationLevel(TxnIsolation.SERIALIZABLE);
        RefCountedConnection rcc=connManager.acquireConnection();
        Assert.assertEquals("Incorrect set isolation",
                TxnIsolation.SERIALIZABLE.level,rcc.element().getTransactionIsolation());
    }

    @Test
    public void commitAdvancesTransactionIdForActiveConnection() throws Exception{
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        long[] txnId = new long[]{0L};
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                txnId[0]++;
                return null;
            }
        }).when(conn).commit();

        Statement s = mock(Statement.class);
        ResultSet mockRs = mock(ResultSet.class);
        when(s.executeQuery("CURRENT_TRANSACTION")).thenReturn(mockRs);
        when(mockRs.next()).thenReturn(true);
        when(mockRs.getLong(1)).thenAnswer(new Answer<Long>(){
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable{
                return txnId[0];
            }
        });
        when(conn.createStatement()).thenReturn(s);

        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);

        RefCountedConnection rcc=connManager.acquireConnection();
        ResultSet rs=rcc.element().createStatement().executeQuery("CURRENT_TRANSACTION");
        rs.next();
        Assert.assertEquals("Is not initial txn id!",0L,rs.getLong(1));
        connManager.commit();
        rs=rcc.element().createStatement().executeQuery("CURRENT_TRANSACTION");
        rs.next();
        Assert.assertEquals("Is not next txn id!",1L,rs.getLong(1));
    }

    @Test
    public void commitAdvancesTransactionIdForInActiveConnection() throws Exception{
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        long[] txnId = new long[]{0L};
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                txnId[0]++;
                return null;
            }
        }).when(conn).commit();

        Statement s = mock(Statement.class);
        ResultSet mockRs = mock(ResultSet.class);
        when(s.executeQuery("CURRENT_TRANSACTION")).thenReturn(mockRs);
        when(mockRs.next()).thenReturn(true);
        when(mockRs.getLong(1)).thenAnswer(new Answer<Long>(){
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable{
                return txnId[0];
            }
        });
        when(conn.createStatement()).thenReturn(s);

        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);

        connManager.commit();
        RefCountedConnection rcc=connManager.acquireConnection();
        ResultSet rs=rcc.element().createStatement().executeQuery("CURRENT_TRANSACTION");
        rs.next();
        Assert.assertEquals("Is not next txn id!",1L,rs.getLong(1));
    }

    @Test
    public void commitThenRollbackOnlyAdvancesOnceForInactiveConnection() throws Exception{
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        long[] txnId = new long[]{0L};
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                txnId[0]++;
                return null;
            }
        }).when(conn).commit();

        Statement s = mock(Statement.class);
        ResultSet mockRs = mock(ResultSet.class);
        when(s.executeQuery("CURRENT_TRANSACTION")).thenReturn(mockRs);
        when(mockRs.next()).thenReturn(true);
        when(mockRs.getLong(1)).thenAnswer(new Answer<Long>(){
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable{
                return txnId[0];
            }
        });
        when(conn.createStatement()).thenReturn(s);

        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);

        connManager.commit();
        connManager.rollback();
        RefCountedConnection rcc=connManager.acquireConnection();
        ResultSet rs=rcc.element().createStatement().executeQuery("CURRENT_TRANSACTION");
        rs.next();
        Assert.assertEquals("Is not next txn id!",1L,rs.getLong(1));

    }

    @Test
    public void rollbackAdvancesTransactionIdForActiveConnection() throws Exception{
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        long[] txnId = new long[]{0L};
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                txnId[0]++;
                return null;
            }
        }).when(conn).rollback();

        Statement s = mock(Statement.class);
        ResultSet mockRs = mock(ResultSet.class);
        when(s.executeQuery("CURRENT_TRANSACTION")).thenReturn(mockRs);
        when(mockRs.next()).thenReturn(true);
        when(mockRs.getLong(1)).thenAnswer(new Answer<Long>(){
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable{
                return txnId[0];
            }
        });
        when(conn.createStatement()).thenReturn(s);

        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);

        RefCountedConnection rcc=connManager.acquireConnection();
        ResultSet rs=rcc.element().createStatement().executeQuery("CURRENT_TRANSACTION");
        rs.next();
        Assert.assertEquals("Is not initial txn id!",0L,rs.getLong(1));
        connManager.rollback();
        rs=rcc.element().createStatement().executeQuery("CURRENT_TRANSACTION");
        rs.next();
        Assert.assertEquals("Is not next txn id!",1L,rs.getLong(1));
    }

    @Test
    public void rollbackAdvancesTransactionIdForInActiveConnection() throws Exception{
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        long[] txnId = new long[]{0L};
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                txnId[0]++;
                return null;
            }
        }).when(conn).rollback();

        Statement s = mock(Statement.class);
        ResultSet mockRs = mock(ResultSet.class);
        when(s.executeQuery("CURRENT_TRANSACTION")).thenReturn(mockRs);
        when(mockRs.next()).thenReturn(true);
        when(mockRs.getLong(1)).thenAnswer(new Answer<Long>(){
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable{
                return txnId[0];
            }
        });
        when(conn.createStatement()).thenReturn(s);

        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);

        connManager.rollback();
        RefCountedConnection rcc=connManager.acquireConnection();
        ResultSet rs=rcc.element().createStatement().executeQuery("CURRENT_TRANSACTION");
        rs.next();
        Assert.assertEquals("Is not next txn id!",1L,rs.getLong(1));
    }

    @Test
    public void rollbackThenCommitOnlyAdvancesOnceForInactiveConnection() throws Exception{
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        long[] txnId = new long[]{0L};
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                txnId[0]++;
                return null;
            }
        }).when(conn).rollback();

        Statement s = mock(Statement.class);
        ResultSet mockRs = mock(ResultSet.class);
        when(s.executeQuery("CURRENT_TRANSACTION")).thenReturn(mockRs);
        when(mockRs.next()).thenReturn(true);
        when(mockRs.getLong(1)).thenAnswer(new Answer<Long>(){
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable{
                return txnId[0];
            }
        });
        when(conn.createStatement()).thenReturn(s);

        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);

        connManager.rollback();
        connManager.commit();
        RefCountedConnection rcc=connManager.acquireConnection();
        ResultSet rs=rcc.element().createStatement().executeQuery("CURRENT_TRANSACTION");
        rs.next();
        Assert.assertEquals("Is not next txn id!",1L,rs.getLong(1));
    }

    @Test
    public void closeClosesActiveConnection() throws Exception{
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        boolean[] close = new boolean[]{false};
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                Assert.assertFalse("Close called multiple times!",close[0]);
                close[0] = true;
                return null;
            }
        }).when(conn).close();

        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);
        @SuppressWarnings("unused") RefCountedConnection rcc=connManager.acquireConnection();
        connManager.close(false);
        Assert.assertTrue("Did not close active connections!",close[0]);
    }

    @Test(expected = IllegalStateException.class)
    public void closePreventsOpeningNewConnections() throws Exception{
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        boolean[] close = new boolean[]{false};
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                Assert.assertFalse("Close called multiple times!",close[0]);
                close[0] = true;
                return null;
            }
        }).when(conn).close();

        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);
        connManager.close(false);
        @SuppressWarnings("unused") RefCountedConnection rcc=connManager.acquireConnection();
    }

    @Test(expected = IllegalStateException.class)
    public void closePreventsCommit() throws Exception{
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        boolean[] close = new boolean[]{false};
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                Assert.assertFalse("Close called multiple times!",close[0]);
                close[0] = true;
                return null;
            }
        }).when(conn).close();

        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);
        connManager.close(false);
        connManager.commit();
    }

    @Test(expected = IllegalStateException.class)
    public void closePreventsRollback() throws Exception{
        ClusteredDataSource cds = mock(ClusteredDataSource.class);
        Connection conn = mock(Connection.class);
        boolean[] close = new boolean[]{false};
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                Assert.assertFalse("Close called multiple times!",close[0]);
                close[0] = true;
                return null;
            }
        }).when(conn).close();

        when(cds.getConnection()).thenReturn(conn);

        ClusteredConnManager connManager = new ClusteredConnManager(cds);
        connManager.close(false);
        connManager.rollback();
    }
}