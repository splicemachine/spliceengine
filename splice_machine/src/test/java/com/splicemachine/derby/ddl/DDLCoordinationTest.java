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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Condition;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.configuration.ConfigurationBuilder;
import com.splicemachine.access.configuration.ConfigurationDefault;
import com.splicemachine.access.configuration.ConfigurationSource;
import com.splicemachine.access.configuration.HConfigurationDefaultsList;
import com.splicemachine.access.util.ReflectingConfigurationSource;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.SingleInstanceLockFactory;
import com.splicemachine.concurrent.TickingClock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.ddl.DDLMessage.DDLChange;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.impl.store.TestingTimestampSource;
import com.splicemachine.si.impl.store.TestingTxnStore;
import com.splicemachine.si.impl.txn.SITransactionReadController;
import com.splicemachine.si.impl.txn.WritableTxn;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import com.splicemachine.util.EmptyConfigurationDefaultsList;
import com.splicemachine.util.concurrent.TestCondition;
import com.splicemachine.util.concurrent.TestLock;

/**
 * Unit tests covering the entire DDL coordination system. In these tests, we simulate the actual communication
 * which occurs between the coordinator and the watcher logic using simulated locks and other stuff like that.
 *
 * @author Scott Fines
 *         Date: 9/8/15
 */
@Category(ArchitectureIndependent.class)
public class DDLCoordinationTest{
    private static final WritableTxn txn=new WritableTxn(0x100L,0x100L,null,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,null,true,null);
    private static final SConfiguration config = new ConfigurationBuilder().build(new EmptyConfigurationDefaultsList().addConfig(new TestConfig()),
                                                                                  new ReflectingConfigurationSource());

    private static final SqlExceptionFactory ef = new SqlExceptionFactory(){
        @Override
        public IOException asIOException(StandardException se){
            return new IOException(se);
        }

        @Override public IOException writeWriteConflict(long txn1,long txn2){ throw new UnsupportedOperationException("Should not happen"); }
        @Override public IOException readOnlyModification(String message){ throw new UnsupportedOperationException("Should not happen"); }
        @Override public IOException noSuchFamily(String message){ throw new UnsupportedOperationException("Should not happen"); }
        @Override public IOException transactionTimeout(long tnxId){ throw new UnsupportedOperationException("Should not happen"); }
        @Override public IOException cannotCommit(long txnId,Txn.State actualState){ throw new UnsupportedOperationException("Should not happen"); }
        @Override public IOException cannotCommit(String message){ throw new UnsupportedOperationException("Should not happen"); }
        @Override public IOException additiveWriteConflict(){ throw new UnsupportedOperationException("Should not happen"); }
        @Override public IOException doNotRetry(String message){ throw new UnsupportedOperationException("Should not happen"); }
        @Override public IOException doNotRetry(Throwable t){ throw new UnsupportedOperationException("Should not happen"); }
        @Override public IOException processRemoteException(Throwable e){ throw new UnsupportedOperationException("Should not happen"); }
        @Override public IOException callerDisconnected(String message){ throw new UnsupportedOperationException("Should not happen"); }
        @Override public IOException failedServer(String message){ throw new UnsupportedOperationException("Should not happen"); }
        @Override public IOException notServingPartition(String s){ throw new UnsupportedOperationException("Should not happen"); }
        @Override public boolean allowsRetry(Throwable error){ throw new UnsupportedOperationException("Should not happen"); }
    };

    @Test
    public void refreshNotifyAfterTimeout() throws Exception{
        /*
         * Test the sequence:
         *
         * C.change  C.waitForResponse ->C.timeout -> R.refresh -> R.notify -> C.fail
         */
        List<String> servers=Collections.singletonList("server1");
        final TickingClock clock=new IncrementingClock(0);

        final List<DDLChange> changes=new ArrayList<>();
        final TestDDLCommunicator ddlCommunicator=getDDLCommunicator(servers,changes);

        final DDLWatchRefresher refresher=getRefresher(clock,changes,ddlCommunicator);
        final CountingListener listener=new CountingListener();

        final TestCondition condition=new TestCondition(clock){
            @Override
            protected void waitUninterruptibly(){
                clock.tickMillis(10l);
                /*
                 * Now that we've moved the clock past the timeout, refresh the DDL
                 */
                try{
                    refresher.refreshDDL(Collections.<DDLWatcher.DDLListener>singleton(listener));
                }catch(IOException e){
                    throw new RuntimeException(e);
                }
            }
        };
        final TestLock lock=new TestLock(clock){
            @Override protected void blockUninterruptibly(){ Assert.fail("Should not block!"); }
            @Override public boolean tryLock(){ return true; }
            @Override public Condition newCondition(){ return condition; }
        };

        AsynchronousDDLController controller=new AsynchronousDDLController(ddlCommunicator,new SingleInstanceLockFactory(lock),clock,config);

        DDLChange change  = ProtoUtil.createNoOpDDLChange(txn.getTxnId(), "testChange",DDLMessage.DDLChangeType.ADD_COLUMN);

        //initiate the sequence
        try{
            String id=controller.notifyMetadataChange(change);
            Assert.fail("Did not time out!");
        }catch(StandardException se){
            Assert.assertEquals("Incorrect error state thrown!","SE017",se.getSQLState());
        }

        //now make sure that the controller had removed the change
        Assert.assertEquals("Change list still contains change!",0,changes.size());
    }


    @Test
    @Ignore("-sf- Ignore until after the merge")
    public void changeRefreshBeforeWait() throws Exception{
        /*
         * Test the sequence:
         *
         * C.change -> R.refresh -> R.notify -> C.waitForResponse -> R.refresh -> C.receiveResponse -> C.finish -> R.refresh
         */
        List<String> servers=Collections.singletonList("server1");
        final TickingClock clock=new IncrementingClock(0);

        final List<DDLChange> changes=new ArrayList<>();
        final TestDDLCommunicator ddlCommunicator=getDDLCommunicator(servers,changes);

        final DDLWatchRefresher refresher=getRefresher(clock,changes,ddlCommunicator);
        final CountingListener listener=new CountingListener();

        final DDLChange change  = ProtoUtil.createNoOpDDLChange(txn.getTxnId(),"testChange",DDLMessage.DDLChangeType.ADD_CHECK);
        final TestCondition condition=new TestCondition(clock){
            @Override
            protected void waitUninterruptibly(){
                /*
                 * We are at the coordinator phase where we are waiting for servers to respond. Therefore,
                 * this is the time where the refresher is determined to refresh and receive the change, responding
                 * appropriately
                 *
                 * In this case, we will have called refresh twice, so first we want to assert that it was
                 * properly notified the first time around
                 */
                assertListenerNotified(listener,change,1,1,0);
                Assert.assertTrue("DDLCommunicator did not receive a notification!",ddlCommunicator.completedServers.containsAll(ddlCommunicator.allServers));
                try{
                    refresher.refreshDDL(Collections.<DDLWatcher.DDLListener>singleton(listener));
                }catch(IOException e){
                    throw new RuntimeException(e);
                }
                clock.tickMillis(4l);
            }
        };
        final TestLock lock=new TestLock(clock){
            boolean locked = false;
            @Override protected void blockUninterruptibly(){ Assert.fail("Should not block!"); }
            @Override public Condition newCondition(){ return condition; }

            @Override
            public boolean tryLock(){
                if(!locked){
                    locked=true;
                    /*
                     * Force the refresh here to simulate the refresh & notification BEFORE the wait acquisition
                     */
                    try{
                        refresher.refreshDDL(Collections.<DDLWatcher.DDLListener>singleton(listener));
                    }catch(IOException e){
                        throw new RuntimeException(e);
                    }
                }
                return true;
            }
        };

        AsynchronousDDLController controller=new AsynchronousDDLController(ddlCommunicator,new SingleInstanceLockFactory(lock),clock,10,100);

        //initiate the sequence
        String id=controller.notifyMetadataChange(change);
        Assert.assertEquals("Incorrect returned id!",change.getChangeId(),id);

        /*
         * Since we refresh twice, we should see that the change has initiated a global start and stop, and there
         * are no active changes for the change itself
         */
        assertListenerNotified(listener,change,0,1,1);
        Assert.assertTrue("DDLCommunicator did not receive a notification!",ddlCommunicator.completedServers.containsAll(ddlCommunicator.allServers));

        /*
         * Now finish the change and make sure the refresher picks up the finish on the next refresh.
         *
         * Note that the coordinator doesn't trigger an automatic removal on the refresher--we have to refresh
         * again to make that happen. In this case, the refresh shouldn't do anything, and therefore shouldn't
         * modify the count.
         */
        controller.finishMetadataChange(change.getChangeId());
        refresher.refreshDDL(Collections.<DDLWatcher.DDLListener>singleton(listener));
        assertListenerNotified(listener,change,0,1,1);
    }


    @Test
    public void changeRefreshNotifyFinishNoErrors() throws Exception{
        /*
         * Test the sequence of events(C = coordinator, R = refresher):
         *
         * C.change -> C.waitForResponse-> R.refresh -> R.notify -> C.receiveResponse-> C.finish ->R.refresh
         */
        List<String> servers=Collections.singletonList("server1");
        final Clock clock=new IncrementingClock(0);

        final List<DDLChange> changes=new ArrayList<>();
        final TestDDLCommunicator ddlCommunicator=getDDLCommunicator(servers,changes);

        final DDLWatchRefresher refresher=getRefresher(clock,changes,ddlCommunicator);
        final CountingListener listener=new CountingListener();

        final TestCondition condition=new TestCondition(clock){
            @Override
            protected void waitUninterruptibly(){
                /*
                 * We are at the coordinator phase where we are waiting for servers to respond. Therefore,
                 * this is the time where the refresher is determined to refresh and receive the change, responding
                 * appropriately
                 */
                try{
                    refresher.refreshDDL(Collections.<DDLWatcher.DDLListener>singleton(listener));
                }catch(IOException e){
                    throw new RuntimeException(e);
                }
            }
        };
        final TestLock lock=new TestLock(clock){
            @Override protected void blockUninterruptibly(){ Assert.fail("Should not block!"); }
            @Override public boolean tryLock(){ return true; }
            @Override public Condition newCondition(){ return condition; }
        };

        AsynchronousDDLController controller=new AsynchronousDDLController(ddlCommunicator,new SingleInstanceLockFactory(lock),clock,10,100);

        DDLChange change  = ProtoUtil.createNoOpDDLChange(txn.getTxnId(), "testChange",DDLMessage.DDLChangeType.ADD_UNIQUE_CONSTRAINT);
        //initiate the sequence
        String id=controller.notifyMetadataChange(change);
        Assert.assertEquals("Incorrect returned id!",change.getChangeId(),id);

        //now assert that the other side received it
        assertListenerNotified(listener,change,1,1,0);
        Assert.assertTrue("DDLCommunicator did not receive a notification!",ddlCommunicator.completedServers.containsAll(ddlCommunicator.allServers));

        /*
         * Now finish the change and make sure the refresher picks up the finish on the next refresh.
         *
         * Note that the coordinator doesn't trigger an automatic removal on the refresher--we have to refresh
         * again to make that happen
         */
        controller.finishMetadataChange(change.getChangeId());
        refresher.refreshDDL(Collections.<DDLWatcher.DDLListener>singleton(listener));
        assertListenerNotified(listener,change,0,1,1);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private DDLWatchRefresher getRefresher(Clock clock,List<DDLChange> changes,final TestDDLCommunicator ddlCommunicator) throws IOException{
        final DDLWatchChecker refreshChecker=getTestChecker("server1",changes,new CommunicationListener(){
            @Override
            public void onCommunicationEvent(String node){
                /*
                 * A little API-hack. Because ZooKeeper only allows a little information in the Watcher,
                 * we need the CommunicationListener api to only hold the node value (in this case, it's the server Id).
                 * However, we need two pieces of information--the changeId, and the server Id. So to do this,
                 * the testChecker hacks the two fields together, separated by a '-'.
                 */
                String[] fields=node.split("-");
                ddlCommunicator.serverCompleted(fields[0],fields[1]);
            }
        });

        TxnStore supplier = new TestingTxnStore(clock,new TestingTimestampSource(),null,100l);
        supplier.recordNewTransaction(txn);
        SITransactionReadController txnController=new SITransactionReadController(supplier);
        return new DDLWatchRefresher(refreshChecker,txnController,clock,ef,10l,supplier);
    }

    private TestDDLCommunicator getDDLCommunicator(final List<String> servers,final List<DDLChange> changes){
        return new TestDDLCommunicator(servers){
            @Override
            public String createChangeNode(DDLChange change) throws StandardException{
                changes.add(change);
                return change.getChangeId();
            }

            @Override
            public void deleteChangeNode(String changeId){
                super.deleteChangeNode(changeId);
                Iterator<DDLChange> iter=changes.iterator();
                while(iter.hasNext()){
                    DDLChange ddlChange=iter.next();
                    if(ddlChange.getChangeId().equals(changeId)){
                        iter.remove();
                        break;
                    }
                }
            }
        };
    }



    private DDLWatchChecker getTestChecker(String serverId,List<DDLChange> changes,CommunicationListener coordinatorListener){
        return new TestChecker(serverId,changes,coordinatorListener);
    }

    private void assertListenerNotified(CountingListener listener,DDLChange change,int eChangeCount,int eGlobalStartCount,int eGlobalStopCount){
        Assert.assertEquals("Refresher did not see change correctly!",eChangeCount,listener.getCount(change));
//        Assert.assertEquals("Refresher's global start count is incorrect!",eGlobalStartCount,listener.getStartGlobalCount());
//        Assert.assertEquals("Refresher's global stop count is incorrect!",eGlobalStopCount,listener.getEndGlobalCount());
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
