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

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 *         Date: 8/16/16
 */
public class ServerPoolTest{
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
                new DeadlineFailureDetector(Long.MAX_VALUE),poolSizingStrategy,blackList,10);

        Connection c = sp.tryAcquireConnection();
        Assert.assertNotNull("Did not return a connection!",c);

        Connection shouldBeNull = sp.tryAcquireConnection();
        Assert.assertNull("Returned too many connections!",shouldBeNull);
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