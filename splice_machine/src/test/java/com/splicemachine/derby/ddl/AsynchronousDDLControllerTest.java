/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.ddl;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Condition;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.configuration.ConfigurationBuilder;
import com.splicemachine.access.configuration.ConfigurationDefault;
import com.splicemachine.access.configuration.ConfigurationSource;
import com.splicemachine.access.util.ReflectingConfigurationSource;
import com.splicemachine.concurrent.LockFactory;
import com.splicemachine.concurrent.SingleInstanceLockFactory;
import com.splicemachine.concurrent.TickingClock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.ddl.DDLMessage.DDLChange;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.txn.WritableTxn;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import com.splicemachine.util.EmptyConfigurationDefaultsList;
import com.splicemachine.util.concurrent.TestCondition;
import com.splicemachine.util.concurrent.TestLock;

/**
 * @author Scott Fines
 *         Date: 9/4/15
 */
@Category(ArchitectureIndependent.class)
public class AsynchronousDDLControllerTest{
    private static final SConfiguration config = new ConfigurationBuilder().build(new EmptyConfigurationDefaultsList().addConfig(new TestConfig()),
                                                                                  new ReflectingConfigurationSource());

    @Test(expected=StandardException.class)
    public void timesOutIfServerRespondsAfterTimeout() throws Exception{
        final List<String> servers = Arrays.asList("server1","server2");
        final String changeId = "change";
        final TickingClock clock = new IncrementingClock(0);
        final long timeout = 10l;

        final TestDDLCommunicator ddlCommunicator = new TestDDLCommunicator(servers){
            @Override
            public String createChangeNode(DDLChange change) throws StandardException{
                return changeId;
            }
        };
        final TestCondition condition = new TestCondition(clock){
            @Override
            protected void waitUninterruptibly(){
                //tick forward the clock to the timeout so that we timeout
                clock.tickMillis(timeout);
                for(String server:servers){
                    ddlCommunicator.serverCompleted(changeId,server);
                }
            }
        };
        //the lock doesn't do any work, it's all in the Condition
        final TestLock lock = new TestLock(clock){
            @Override protected void blockUninterruptibly(){ }
            @Override public boolean tryLock(){ return true; } //always allow the lock access
            @Override public Condition newCondition(){ return condition; }
        };

        LockFactory lf = new SingleInstanceLockFactory(lock);

        AsynchronousDDLController controller=new AsynchronousDDLController(ddlCommunicator,lf,clock,config);
        TxnView txn = new WritableTxn(1l,1l,null,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,null,false,null);
        DDLChange change = ProtoUtil.createNoOpDDLChange(txn.getTxnId(),"testChange",DDLMessage.DDLChangeType.CREATE_INDEX);
        try{
            controller.notifyMetadataChange(change);
            Assert.fail("The coordination did not time out!");
        }catch(StandardException se){
            Assert.assertEquals("Incorrect error code!","SE017",se.getSQLState());
            throw se;
        }
    }

    @Test(expected=StandardException.class)
    public void timesOutIfNoServerEverResponds() throws Exception{
        final List<String> servers = Arrays.asList("server1","server2");
        final String changeId = "change";
        final TickingClock clock = new IncrementingClock(0);
        final long timeout = 10l;

        final TestDDLCommunicator ddlCommunicator = new TestDDLCommunicator(servers){
            @Override
            public String createChangeNode(DDLChange change) throws StandardException{
                return changeId;
            }
        };
        final TestCondition condition = new TestCondition(clock){
            @Override
            protected void waitUninterruptibly(){
                //tick forward the clock to the timeout so that we timeout
                clock.tickMillis(timeout);
            }
        };
        //the lock doesn't do any work, it's all in the Condition
        final TestLock lock = new TestLock(clock){
            @Override protected void blockUninterruptibly(){ }
            @Override public boolean tryLock(){ return true; } //always allow the lock access
            @Override public Condition newCondition(){ return condition; }
        };

        LockFactory lf = new SingleInstanceLockFactory(lock);

        AsynchronousDDLController controller=new AsynchronousDDLController(ddlCommunicator,lf,clock,config);
        TxnView txn = new WritableTxn(1l,1l,null,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,null,false,null);
        DDLChange change = ProtoUtil.createNoOpDDLChange(txn.getTxnId(),"testChange",DDLMessage.DDLChangeType.CHANGE_PK );
        try{
            controller.notifyMetadataChange(change);
            Assert.fail("The coordination did not time out!");
        }catch(StandardException se){
            Assert.assertEquals("Incorrect error code!","SE017",se.getSQLState());
            throw se;
        }
    }

    @Test
    public void correctlyIgnoresRemovedServers() throws Exception{
        final List<String> servers = Arrays.asList("server1","server2");
        final String changeId = "change";
        final TickingClock clock = new IncrementingClock(0);
        final long timeout = 10l;

        final TestDDLCommunicator ddlCommunicator = new TestDDLCommunicator(servers){
            @Override
            public String createChangeNode(DDLChange change) throws StandardException{
                return changeId;
            }
        };
        final TestCondition condition = new TestCondition(clock){
            int state = 0;
            @Override
            protected void waitUninterruptibly(){
                switch(state){
                    case 0:
                        ddlCommunicator.serverCompleted(changeId,servers.get(0));
                        break;
                    case 1:
                        ddlCommunicator.decommissionServer("server2");
                        break;
                    default:
                        Assert.fail("Unexpected call to wait()");
                }
                clock.tickMillis(timeout/5);
                state++;
            }
        };
        //the lock doesn't do any work, it's all in the Condition
        final TestLock lock = new TestLock(clock){
            @Override protected void blockUninterruptibly(){ }
            @Override public boolean tryLock(){ return true; } //always allow the lock access
            @Override public Condition newCondition(){ return condition; }
        };

        LockFactory lf = new SingleInstanceLockFactory(lock);

        AsynchronousDDLController controller=new AsynchronousDDLController(ddlCommunicator,lf,clock,10,100);
        TxnView txn = new WritableTxn(1l,1l,null,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,null,false,null);
        DDLChange change = ProtoUtil.createNoOpDDLChange(txn.getTxnId(),"testChange",DDLMessage.DDLChangeType.ADD_COLUMN);
        String retChangeId=controller.notifyMetadataChange(change);
        Assert.assertEquals("Change id does not match!",changeId,retChangeId);
        Assert.assertTrue("Some servers are missing!",ddlCommunicator.completedServers.containsAll(ddlCommunicator.allServers));
    }

    @Test
    public void correctlyCatchesNewServers() throws Exception{
        final List<String> servers = Arrays.asList("server1","server2");
        final String changeId = "change";
        final TickingClock clock = new IncrementingClock(0);
        final long timeout = 10l;

        final TestDDLCommunicator ddlCommunicator = new TestDDLCommunicator(servers){
            @Override
            public String createChangeNode(DDLChange change) throws StandardException{
                return changeId;
            }
        };
        final TestCondition condition = new TestCondition(clock){
            int state = 0;
            @Override
            protected void waitUninterruptibly(){
                switch(state){
                    case 0:
                        ddlCommunicator.serverCompleted(changeId,servers.get(0));
                        break;
                    case 1:
                        ddlCommunicator.commissionServer("server3");
                        break;
                    case 2:
                        ddlCommunicator.serverCompleted(changeId,servers.get(1));
                        break;
                    case 3:
                        ddlCommunicator.serverCompleted(changeId,"server3");
                        break;
                    default:
                        Assert.fail("Unexpected call to wait()");
                }
                clock.tickMillis(timeout/5);
                state++;
            }
        };
        //the lock doesn't do any work, it's all in the Condition
        final TestLock lock = new TestLock(clock){
            @Override protected void blockUninterruptibly(){ }
            @Override public boolean tryLock(){ return true; } //always allow the lock access
            @Override public Condition newCondition(){ return condition; }
        };

        LockFactory lf = new SingleInstanceLockFactory(lock);

        AsynchronousDDLController controller=new AsynchronousDDLController(ddlCommunicator,lf,clock,10,100);
        TxnView txn = new WritableTxn(1l,1l,null,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,null,false,null);
        DDLChange change = ProtoUtil.createNoOpDDLChange(txn.getTxnId(),"testChange",DDLMessage.DDLChangeType.ALTER_STATS);
        String retChangeId=controller.notifyMetadataChange(change);
        Assert.assertEquals("Change id does not match!",changeId,retChangeId);
        Assert.assertTrue("Some servers are missing!",ddlCommunicator.completedServers.containsAll(ddlCommunicator.allServers));
    }

    @Test(expected=StandardException.class)
    public void timesOutIfOneServerNeverResponds() throws Exception{
        final List<String> servers = Arrays.asList("server1","server2");
        final String changeId = "change";
        final TickingClock clock = new IncrementingClock(0);
        final long timeout = 10l;

        final TestDDLCommunicator ddlCommunicator = new TestDDLCommunicator(servers){
            @Override public String createChangeNode(DDLChange change) throws StandardException{ return changeId; }
        };
        final TestCondition condition = new TestCondition(clock){
            int state = 0;
            @Override
            protected void waitUninterruptibly(){
                switch(state){
                    case 0:
                        //make just one server respond
                        ddlCommunicator.serverCompleted(changeId,servers.get(0));
                        break;
                    case 1:
                        clock.tickMillis(timeout); //force the timeout
                        break;
                    default:
                        Assert.fail("wait() called after timing out");
                }
                clock.tickMillis(timeout/3);
                state++;
            }
        };
        //the lock doesn't do any work, it's all in the Condition
        final TestLock lock = new TestLock(clock){
            @Override protected void blockUninterruptibly(){ }
            @Override public boolean tryLock(){ return true; } //always allow the lock access
            @Override public Condition newCondition(){ return condition; }
        };

        LockFactory lf = new SingleInstanceLockFactory(lock);

        AsynchronousDDLController controller=new AsynchronousDDLController(ddlCommunicator,lf,clock,config);
        TxnView txn = new WritableTxn(1l,1l,null,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,null,false,null);
        DDLChange change = ProtoUtil.createNoOpDDLChange(txn.getTxnId(),"testChange",DDLMessage.DDLChangeType.ADD_UNIQUE_CONSTRAINT );
        try{
            controller.notifyMetadataChange(change);
            Assert.fail("The coordination did not time out!");
        }catch(StandardException se){
            Assert.assertEquals("Incorrect error code!","SE017",se.getSQLState());
            throw se;
        }
    }

    @Test
    public void noErrorWhenOneServerBecomesInactive() throws Exception{
        final List<String> servers = Arrays.asList("server1","server2");
        final String changeId = "change";
        TickingClock clock = new IncrementingClock(0);

        final TestDDLCommunicator ddlCommunicator = new TestDDLCommunicator(servers){
            @Override public String createChangeNode(DDLChange change) throws StandardException{ return changeId; }
        };
        final TestCondition condition = new TestCondition(clock){
            @Override
            protected void waitUninterruptibly(){
                ddlCommunicator.serverCompleted(changeId,servers.get(0));
                ddlCommunicator.decommissionServer(servers.get(1));
            }
        };
        //the lock doesn't do any work, it's all in the Condition
        final TestLock lock = new TestLock(clock){
            @Override protected void blockUninterruptibly(){ }
            @Override public boolean tryLock(){ return true; }
            @Override public Condition newCondition(){ return condition; }
        };

        LockFactory lf = new SingleInstanceLockFactory(lock);

        AsynchronousDDLController controller=new AsynchronousDDLController(ddlCommunicator,lf,clock,100,10);
        TxnView txn = new WritableTxn(1l,1l,null,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,null,false,null);
        DDLChange change = ProtoUtil.createNoOpDDLChange(txn.getTxnId(),"testChange",DDLMessage.DDLChangeType.CREATE_SCHEMA );
        String retChangeId=controller.notifyMetadataChange(change);
        Assert.assertEquals("Change id does not match!",changeId,retChangeId);
        Assert.assertTrue("Some servers are missing!",ddlCommunicator.completedServers.containsAll(ddlCommunicator.allServers));
    }

    @Test
    public void noErrorWhenAllServersRespondBeforeFirstWait() throws Exception{
        final List<String> servers = Arrays.asList("server1","server2");
        final String changeId = "change";
        TickingClock clock = new IncrementingClock(0);

        final TestDDLCommunicator ddlCommunicator = new TestDDLCommunicator(servers){
            @Override public String createChangeNode(DDLChange change) throws StandardException{ return changeId; }
        };
        //add the server responses
        for(String server:servers){
            ddlCommunicator.serverCompleted(changeId,server);
        }
        final TestCondition condition = new TestCondition(clock){
            @Override
            protected void waitUninterruptibly(){
                Assert.fail("Should not wait on the condition!");
            }
        };
        final TestLock lock = new TestLock(clock){
            //the lock doesn't do any work, it's all in the Condition
            @Override protected void blockUninterruptibly(){ }
            //always allow the lock through
            @Override public boolean tryLock(){
                Assert.fail("Should not have tried the lock!");
                return false; //not reached
            }

            @Override public Condition newCondition(){ return condition; }
        };

        LockFactory lf = new SingleInstanceLockFactory(lock);

        AsynchronousDDLController controller=new AsynchronousDDLController(ddlCommunicator,lf,clock,10,100);
        TxnView txn = new WritableTxn(1l,1l,null,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,null,false,null);
        DDLChange change = ProtoUtil.createNoOpDDLChange(txn.getTxnId(),"testChange",DDLMessage.DDLChangeType.CREATE_SCHEMA);
        String retChangeId=controller.notifyMetadataChange(change);
        Assert.assertEquals("Change id does not match!",changeId,retChangeId);
        Assert.assertTrue("Some servers are missing!",ddlCommunicator.completedServers.containsAll(ddlCommunicator.allServers));
    }

    @Test
    public void testWorksWhenAllServersRespondAfterFirstCheck() throws Exception{
        final List<String> servers = Arrays.asList("server1","server2");
        final String changeId = "change";
        final TestDDLCommunicator ddlCommunicator = new TestDDLCommunicator(servers){
            @Override public String createChangeNode(DDLChange change) throws StandardException{ return changeId; }
        };
        TickingClock clock = new IncrementingClock(0);
        final TestCondition condition = new TestCondition(clock){
            @Override
            protected void waitUninterruptibly(){
                //add the server responses
                for(String server:servers){
                    ddlCommunicator.serverCompleted(changeId,server);
                }
            }
        };
        final TestLock lock = new TestLock(clock){
            @Override protected void blockUninterruptibly(){ }
            @Override public boolean tryLock(){ return true; } //always allow the lock access
            @Override public Condition newCondition(){ return condition; }
        };

        LockFactory lf = new SingleInstanceLockFactory(lock);

        AsynchronousDDLController controller=new AsynchronousDDLController(ddlCommunicator,lf,clock,100,10);
        TxnView txn = new WritableTxn(1l,1l,null,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,null,false,null);
        DDLChange change = ProtoUtil.createNoOpDDLChange(txn.getTxnId(),"testCHange",DDLMessage.DDLChangeType.ADD_NOT_NULL);
        String retChangeId=controller.notifyMetadataChange(change);
        Assert.assertEquals("Change id does not match!",changeId,retChangeId);
        Assert.assertTrue("Some servers are missing!",ddlCommunicator.completedServers.containsAll(ddlCommunicator.allServers));
    }

    //==============================================================================================================
    // private helper classes
    //==============================================================================================================
    private static class TestConfig implements ConfigurationDefault {

        @Override
        public void setDefaults(ConfigurationBuilder builder, ConfigurationSource configurationSource) {
            // Overwritten for test
            builder.ddlRefreshInterval = 5L;
            builder.maxDdlWait = 10L;
        }
    }

}