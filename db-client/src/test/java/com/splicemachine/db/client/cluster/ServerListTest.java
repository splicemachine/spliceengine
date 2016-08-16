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

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * @author Scott Fines
 *         Date: 9/13/16
 */
public class ServerListTest{

    @Test
    public void functionsWithEmptyServerList() throws Exception{
        ServerList sl = new ServerList((previous,numServers)->1,new ServerPool[]{});

        Iterator<ServerPool> spIter=sl.activeServers();
        Assert.assertFalse("Returns a server!",spIter.hasNext());
    }

    @Test
    public void activeServersReturnsActive() throws Exception{
        FailureDetector fd = new DeadlineFailureDetector(Long.MAX_VALUE);
        ServerPool active = new ServerPool(mock(DataSource.class),
                "testServer",1,fd,mock(PoolSizingStrategy.class),Integer.MAX_VALUE);

        ServerList sl = new ServerList((previous,numServers)->1,new ServerPool[]{active});

        Iterator<ServerPool> spIter=sl.activeServers();
        Assert.assertTrue("Did not see active servers!",spIter.hasNext());
        ServerPool sp = spIter.next();
        Assert.assertEquals("Incorrect server pool!","testServer",sp.serverName);
        Assert.assertFalse("Server reported as dead!",sp.isDead());
        Assert.assertFalse("Has more than 1 server!",spIter.hasNext());
    }

    @Test
    public void filtersOutKnownDeadServers() throws Exception{
        FailureDetector fd = new DeadlineFailureDetector(0);
        ServerPool active = new ServerPool(mock(DataSource.class),
                "testServer",1,fd,mock(PoolSizingStrategy.class),Integer.MAX_VALUE);

        ServerList sl = new ServerList((previous,numServers)->1,new ServerPool[]{active});

        Iterator<ServerPool> spIter=sl.activeServers();
        Assert.assertFalse("Has a server!",spIter.hasNext());
    }

    @Test
    public void filtersOutKnownDeadServersWithGoodOnes() throws Exception{
        FailureDetector fd = new DeadlineFailureDetector(0);
        ServerPool dead = new ServerPool(mock(DataSource.class),
                "deadServer",1,fd,mock(PoolSizingStrategy.class),Integer.MAX_VALUE);
        ServerPool active = new ServerPool(mock(DataSource.class),
                "activeServer",1,new DeadlineFailureDetector(Long.MAX_VALUE),
                mock(PoolSizingStrategy.class),Integer.MAX_VALUE);

        ServerList sl = new ServerList((previous,numServers)->1,new ServerPool[]{dead,active});

        Iterator<ServerPool> spIter=sl.activeServers();
        Assert.assertTrue("Did not see active servers!",spIter.hasNext());
        ServerPool sp = spIter.next();
        Assert.assertEquals("Incorrect server pool!",active.serverName,sp.serverName);
        Assert.assertFalse("Server reported as dead!",sp.isDead());
        Assert.assertFalse("Has more than 1 server!",spIter.hasNext());
    }

    @Test
    public void returnsNothingWhenEmpty() throws Exception{

        ServerList sl = new ServerList((previous,numServers)->1,new ServerPool[]{});

        Iterator<ServerPool> spIter=sl.activeServers();
        Assert.assertFalse("Has a server!",spIter.hasNext());
    }

    @Test
    public void heartbeatFindsDeadServers() throws Exception{
        final FailureDetector fd = new DeadlineFailureDetector(Long.MAX_VALUE);
        ServerPool active = new ServerPool(mock(DataSource.class),
                "testServer",1,fd,mock(PoolSizingStrategy.class),Integer.MAX_VALUE);
        ServerPool mock = spy(active);
        doAnswer(new Answer<Boolean>(){
            @Override
            public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable{
                fd.kill();
                return false;
            }
        }).when(mock).heartbeat();

        ServerList sl = new ServerList((previous,numServers)->1,new ServerPool[]{mock});

        Iterator<ServerPool> spIter=sl.activeServers();
        Assert.assertTrue("Did not see active servers!",spIter.hasNext());
        ServerPool sp = spIter.next();
        Assert.assertEquals("Incorrect server pool!","testServer",sp.serverName);
        Assert.assertFalse("Server reported as dead!",sp.isDead());
        Assert.assertFalse("Has more than 1 server!",spIter.hasNext());

        //now force a heartbeat
        sl.validateAllServers();
        spIter=sl.activeServers();
        Assert.assertFalse("Did not remove dead server!",spIter.hasNext());
    }

    @Test
    public void newServerListRemovesOldServers() throws Exception{
        DeadlineFailureDetector failureDetector=new DeadlineFailureDetector(1){
            @Override public boolean isAlive(){ return false; }
        };
        ServerPool dead = new ServerPool(mock(DataSource.class),
                "deadServer",1,failureDetector,mock(PoolSizingStrategy.class),Integer.MAX_VALUE);
        ServerPool active = new ServerPool(mock(DataSource.class),
                "activeServer",1,new DeadlineFailureDetector(Long.MAX_VALUE),
                mock(PoolSizingStrategy.class),Integer.MAX_VALUE);

        ServerList sl = new ServerList((previous,numServers)->1,new ServerPool[]{dead,active});

        sl.setServerList(new ServerPool[]{active});

        Iterator<ServerPool> spIter=sl.activeServers();
        Assert.assertTrue("Did not see active servers!",spIter.hasNext());
        ServerPool sp = spIter.next();
        Assert.assertEquals("Incorrect server pool!","activeServer",sp.serverName);
        Assert.assertFalse("Server reported as dead!",sp.isDead());
        Assert.assertFalse("Has more than 1 server!",spIter.hasNext());
    }

    @Test
    public void newServerListAddsNewServers() throws Exception{
        DeadlineFailureDetector failureDetector=new DeadlineFailureDetector(1){
            @Override public boolean isAlive(){ return false; }
        };
        ServerPool dead = new ServerPool(mock(DataSource.class),
                "deadServer",1,failureDetector,mock(PoolSizingStrategy.class),Integer.MAX_VALUE);
        ServerPool active = new ServerPool(mock(DataSource.class),
                "activeServer",1,new DeadlineFailureDetector(Long.MAX_VALUE),
                mock(PoolSizingStrategy.class),Integer.MAX_VALUE);

        ServerList sl = new ServerList((previous,numServers)->1,new ServerPool[]{dead,active});

        Iterator<ServerPool> spIter=sl.activeServers();
        Assert.assertTrue("Did not see active servers!",spIter.hasNext());
        ServerPool sp = spIter.next();
        Assert.assertEquals("Incorrect server pool!","activeServer",sp.serverName);
        Assert.assertFalse("Server reported as dead!",sp.isDead());
        Assert.assertFalse("Has more than 1 server!",spIter.hasNext());

        ServerPool active2 = new ServerPool(mock(DataSource.class),
                "activeServer2",1,new DeadlineFailureDetector(Long.MAX_VALUE),
                mock(PoolSizingStrategy.class),Integer.MAX_VALUE);

        sl.setServerList(new ServerPool[]{active,active2});

        List<ServerPool> sps = new ArrayList<>();
        spIter=sl.activeServers();
        while(spIter.hasNext()){
            sps.add(spIter.next());
        }
        Assert.assertEquals("Incorrect size!",2,sps.size());
        Collections.sort(sps);
        Assert.assertEquals("Missing active server!",active,sps.get(0));
        Assert.assertEquals("Missing active2 server!",active2,sps.get(1));
    }

    @Test
    public void addServerAddsNewServers() throws Exception{
        DeadlineFailureDetector failureDetector=new DeadlineFailureDetector(1){
            @Override public boolean isAlive(){ return false; }
        };
        ServerPool dead = new ServerPool(mock(DataSource.class),
                "deadServer",1,failureDetector,mock(PoolSizingStrategy.class),Integer.MAX_VALUE);
        ServerPool active = new ServerPool(mock(DataSource.class),
                "activeServer",1,new DeadlineFailureDetector(Long.MAX_VALUE),
                mock(PoolSizingStrategy.class),Integer.MAX_VALUE);

        ServerList sl = new ServerList((previous,numServers)->1,new ServerPool[]{dead,active});

        Iterator<ServerPool> spIter=sl.activeServers();
        Assert.assertTrue("Did not see active servers!",spIter.hasNext());
        ServerPool sp = spIter.next();
        Assert.assertEquals("Incorrect server pool!","activeServer",sp.serverName);
        Assert.assertFalse("Server reported as dead!",sp.isDead());
        Assert.assertFalse("Has more than 1 server!",spIter.hasNext());

        ServerPool active2 = new ServerPool(mock(DataSource.class),
                "activeServer2",1,new DeadlineFailureDetector(Long.MAX_VALUE),
                mock(PoolSizingStrategy.class),Integer.MAX_VALUE);

        sl.addServer(active2);

        List<ServerPool> sps = new ArrayList<>();
        spIter=sl.activeServers();
        while(spIter.hasNext()){
            sps.add(spIter.next());
        }
        Assert.assertEquals("Incorrect size!",2,sps.size());
        Collections.sort(sps);
        Assert.assertEquals("Missing active server!",active,sps.get(0));
        Assert.assertEquals("Missing active2 server!",active2,sps.get(1));
    }

    @Test
    public void repeatedAddServerAddsNewServer() throws Exception{
        for(int i=0;i<100;i++){
            addServerAddsNewServers();
        }
    }
}