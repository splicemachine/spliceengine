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

import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.TickingClock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.impl.store.TestingTimestampSource;
import com.splicemachine.si.impl.store.TestingTxnStore;
import com.splicemachine.si.impl.txn.SITransactionReadController;
import com.splicemachine.si.impl.txn.WritableTxn;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * @author Scott Fines
 *         Date: 9/7/15
 */
@Category(ArchitectureIndependent.class)
public class DDLWatchRefresherTest{

    private static final WritableTxn txn=new WritableTxn(0x100l,0x100l,null,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,null,true,null);
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
    public void picksUpNonTentativeChange() throws Exception{
        TestChecker checker=getTestChecker();
        Clock clock = new IncrementingClock(0);

        TxnStore supplier = new TestingTxnStore(clock,new TestingTimestampSource(),null,100l);
        supplier.recordNewTransaction(txn);
        DDLWatchRefresher refresher = new DDLWatchRefresher(checker,null,clock,ef,10l,supplier);

        //add a new change

        final DDLChange testChange  = ProtoUtil.createNoOpDDLChange(txn.getTxnId(),"change",DDLMessage.DDLChangeType.CHANGE_PK);
        checker.addChange(testChange);

        //now refresh, and make sure it gets picked up
        CountingListener assertionListener = new CountingListener();
        boolean shouldCont=refresher.refreshDDL(Collections.<DDLWatcher.DDLListener>singleton(assertionListener));
        Assert.assertTrue("Returned an error State!",shouldCont);
        /*
         * We haven't "finished" the ddl change yet, so we should expect to have seen only a start, and a global
         * start (e.g. no global stop, and a change count of 1)
         */
        Assert.assertEquals("Incorrect initiated count!",1,assertionListener.getCount(testChange));
//        Assert.assertEquals("Incorrect global start count!",1,assertionListener.getStartGlobalCount());
//        Assert.assertEquals("Incorrect global stop count!",0,assertionListener.getEndGlobalCount());
    }


    @Test
    public void allPostCommitAreTreatedAsSuch() throws Exception{
        TestChecker checker=getTestChecker();
        Clock clock = new IncrementingClock(0);

        TxnStore supplier = new TestingTxnStore(clock,new TestingTimestampSource(),null,100l);
        supplier.recordNewTransaction(txn);
        SITransactionReadController txnController=new SITransactionReadController(supplier);
        DDLWatchRefresher refresher = new DDLWatchRefresher(checker,txnController,clock,ef,10l,supplier );
        CountingListener assertionListener = new CountingListener();

        for(DDLChangeType type:DDLChangeType.values()){
            if(type.isPreCommit()) continue; //ignore tentative ones for this test

            //add a new change
            final DDLChange testChange  = ProtoUtil.createNoOpDDLChange(txn.getTxnId(),type.toString(),DDLMessage.DDLChangeType.CREATE_SCHEMA);
            checker.addChange(testChange);

            //now refresh, and make sure it gets picked up
            boolean shouldCont=refresher.refreshDDL(Collections.<DDLWatcher.DDLListener>singleton(assertionListener));
            Assert.assertTrue("Returned an error State!",shouldCont);
            /*
             * We haven't "finished" the ddl change yet, so we should expect to have seen only a start, and a global
             * start (e.g. no global stop, and a change count of 1)
             */
            Assert.assertEquals("Incorrect initiated count for changeType "+type+"!",1,assertionListener.getCount(testChange));
            //there should only be 1 global change initiated, no matter what
            Assert.assertEquals("Incorrect global start count!",0,assertionListener.getStartGlobalCount());
            Assert.assertEquals("Incorrect global stop count!",0,assertionListener.getEndGlobalCount());
//            Collection<DDLChange> tentativeChanges = refresher.tentativeDDLChanges();
//            assertFalse("picked up "+type+" as tentative!",tentativeChanges.contains(testChange));
        }
    }

    @Test
    public void allPreCommitAreTreatedAsSuch() throws Exception{
        TestChecker checker=getTestChecker();
        Clock clock = new IncrementingClock(0);

        TxnStore supplier = new TestingTxnStore(clock,new TestingTimestampSource(),null,100l);
        supplier.recordNewTransaction(txn);
        SITransactionReadController txnController=new SITransactionReadController(supplier);
        DDLWatchRefresher refresher = new DDLWatchRefresher(checker,txnController,clock,ef,10l,supplier);
        CountingListener assertionListener = new CountingListener();

        for(DDLChangeType type:DDLChangeType.values()){
            if(type.isPostCommit()) continue; //ignore tentative ones for this test

            //add a new change
            DDLChange testChange  = ProtoUtil.createNoOpDDLChange(txn.getTxnId(),type.toString(),DDLMessage.DDLChangeType.ADD_COLUMN );
            checker.addChange(testChange);

            //now refresh, and make sure it gets picked up
            boolean shouldCont=refresher.refreshDDL(Collections.<DDLWatcher.DDLListener>singleton(assertionListener));
            Assert.assertTrue("Returned an error State!",shouldCont);
            /*
             * We haven't "finished" the ddl change yet, so we should expect to have seen only a start, and a global
             * start (e.g. no global stop, and a change count of 1)
             */
            Assert.assertEquals("Incorrect initiated count for changeType "+type+"!",1,assertionListener.getCount(testChange));
            Assert.assertEquals("Incorrect global start count!",0,assertionListener.getStartGlobalCount());
            Assert.assertEquals("Incorrect global stop count!",0,assertionListener.getEndGlobalCount());
            Collection<DDLChange> tentativeChanges = refresher.tentativeDDLChanges();
            assertTrue("picked up "+type+" as tentative!",tentativeChanges.contains(testChange));
        }
    }

    @Test
    public void removesFinishedChange() throws Exception{
        TestChecker checker=getTestChecker();
        Clock clock = new IncrementingClock(0);

        TxnStore supplier = new TestingTxnStore(clock,new TestingTimestampSource(),null,100l);
        supplier.recordNewTransaction(txn);
        SITransactionReadController txnController=new SITransactionReadController(supplier);
        DDLWatchRefresher refresher = new DDLWatchRefresher(checker,txnController,clock,ef,10l,supplier);

        //add a new change
        DDLChange testChange  = ProtoUtil.createNoOpDDLChange(txn.getTxnId(),"change",DDLMessage.DDLChangeType.CHANGE_PK );
        checker.addChange(testChange);

        //now refresh, and make sure it gets picked up
        CountingListener assertionListener = new CountingListener();
        boolean shouldCont=refresher.refreshDDL(Collections.<DDLWatcher.DDLListener>singleton(assertionListener));
        Assert.assertTrue("Returned an error State!",shouldCont);
        /*
         * We haven't "finished" the ddl change yet, so we should expect to have seen only a start, and a global
         * start (e.g. no global stop, and a change count of 1)
         */
        Assert.assertEquals("Incorrect initiated count!",1,assertionListener.getCount(testChange));
//        Assert.assertEquals("Incorrect global start count!",1,assertionListener.getStartGlobalCount());
//        Assert.assertEquals("Incorrect global stop count!",0,assertionListener.getEndGlobalCount());

        //refresh again without adding to the list--this emulates the initiator cleaning itself up nicely
        shouldCont = refresher.refreshDDL(Collections.<DDLWatcher.DDLListener>singleton(assertionListener));
        Assert.assertTrue("Returned an error State!",shouldCont);
        /*
         * We "finished" the ddl change (because we visited it twice), so we should see a count of 0, and a global
         * count of 1 in both cases
         */
        Assert.assertEquals("Incorrect initiated count!",0,assertionListener.getCount(testChange));
//        Assert.assertEquals("Incorrect global start count!",1,assertionListener.getStartGlobalCount());
//        Assert.assertEquals("Incorrect global stop count!",1,assertionListener.getEndGlobalCount());
    }

    @Test
    public void testDDLTimesOut() throws Exception{
        /*
         * Test that we do the right thing when we time out on a ddl operation (e.g. when the initiator of the
         * change then dies and leaves behind junk that we have to clean up).
         *
         * This timeout is initiated only after it has been initiated, but failed to be cleaned up. In distributed
         * terms, one server received the initial request, but the initiating node failed before it could complete
         * the operation. In this case, we timeout, but only if it is still there
         */
        TestChecker checker=getTestChecker();
        TickingClock clock = new IncrementingClock(0);

        TxnStore supplier = new TestingTxnStore(clock,new TestingTimestampSource(),null,100l);
        supplier.recordNewTransaction(txn);
        long timeoutMs=10l;
        DDLWatchRefresher refresher = new DDLWatchRefresher(checker,null,clock,ef,timeoutMs,supplier);
        //add a new change
        final DDLChange testChange  = ProtoUtil.createNoOpDDLChange(txn.getTxnId(),"change",DDLMessage.DDLChangeType.ADD_PRIMARY_KEY );
        CountingListener assertionListener = new CountingListener();

        //check it and run it
        checker.addChange(testChange);
        boolean shouldCont=refresher.refreshDDL(Collections.<DDLWatcher.DDLListener>singleton(assertionListener));
        Assert.assertTrue("Returned an error State!",shouldCont);
        /*
         * We haven't "finished" the ddl change yet, so we should expect to have seen only a start, and a global
         * start (e.g. no global stop, and a change count of 1)
         */
        Assert.assertEquals("Incorrect initiated count!",1,assertionListener.getCount(testChange));
//        Assert.assertEquals("Incorrect global start count!",1,assertionListener.getStartGlobalCount());
//        Assert.assertEquals("Incorrect global stop count!",0,assertionListener.getEndGlobalCount());

        //move the clock forward
        clock.tickMillis(5l);
        /*
         * To simulate a change within the timeout hanging around, add it back to the checker, then refresh
         * and make sure it wasn't re-processed. Nothing should have changed
         */
        checker.addChange(testChange);
        shouldCont=refresher.refreshDDL(Collections.<DDLWatcher.DDLListener>singleton(assertionListener));
        Assert.assertTrue("Returned an error State!",shouldCont);
        /*
         * We haven't "finished" the ddl change yet, so we should expect to have seen only a start, and a global
         * start (e.g. no global stop, and a change count of 1)
         */
        Assert.assertEquals("Incorrect initiated count!",1,assertionListener.getCount(testChange));
//        Assert.assertEquals("Incorrect global start count!",1,assertionListener.getStartGlobalCount());
//        Assert.assertEquals("Incorrect global stop count!",0,assertionListener.getEndGlobalCount());

        /*
         * Now move the clock forward past the timeout position, and refresh again, making sure that it was terminated
         */
        clock.tickMillis(10);
        shouldCont=refresher.refreshDDL(Collections.<DDLWatcher.DDLListener>singleton(assertionListener));
        Assert.assertTrue("Returned an error State!",shouldCont);
        /*
         * We haven't "finished" the ddl change yet, so we should expect to have seen only a start, and a global
         * start (e.g. no global stop, and a change count of 1)
         */
        Assert.assertEquals("Incorrect initiated count!",1,assertionListener.getCount(testChange));
        Assert.assertTrue("Incorrect failed count!",assertionListener.isFailed(testChange));
//        Assert.assertEquals("Incorrect global start count!",1,assertionListener.getStartGlobalCount());
//        Assert.assertEquals("Incorrect global stop count!",1,assertionListener.getEndGlobalCount());
    }

    /* ****************************************************************************************************************/
    /*private helper classes and methods*/




    private TestChecker getTestChecker() throws IOException{
        TestChecker checker = new TestChecker();
        CommunicationListener noop=new CommunicationListener(){
            @Override public void onCommunicationEvent(String node){
                Assert.fail("Received an unexpected communication event!");
            }
        };
        checker.initialize(noop);
        return checker;
    }

}