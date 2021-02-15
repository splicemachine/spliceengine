/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.si;

import com.carrotsearch.hppc.LongArrayList;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TransactionMissing;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.ForwardingLifecycleManager;
import com.splicemachine.si.impl.ForwardingTxnView;
import com.splicemachine.si.testenv.ArchitectureSpecific;
import com.splicemachine.si.testenv.SITestEnv;
import com.splicemachine.si.testenv.SITestEnvironment;
import com.splicemachine.si.testenv.TestTransactionSetup;
import com.splicemachine.timestamp.api.TimestampSource;
import org.junit.*;
import org.junit.experimental.categories.Category;
import splice.com.google.common.collect.Lists;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Tests around the logic for fetching active transactions.
 *
 * @author Scott Fines
 *         Date: 8/21/14
 */
@Category(ArchitectureSpecific.class)
public class ActiveTransactionTest{

    private static final byte[] DESTINATION_TABLE=Bytes.toBytes("1216");

    private static SITestEnv testEnv;
    private static TestTransactionSetup transactorSetup;

    private TxnLifecycleManager control;
    private final List<Txn> createdParentTxns= Lists.newArrayList();
    private TxnStore txnStore;

    @Before
    public void setUp() throws Exception{
        if(testEnv==null){
            testEnv=SITestEnvironment.loadTestEnvironment();
            transactorSetup=new TestTransactionSetup(testEnv,true);
        }
        control=new ForwardingLifecycleManager(transactorSetup.txnLifecycleManager){
            @Override
            protected void afterStart(Txn txn){
                createdParentTxns.add(txn);
            }
        };
        txnStore=transactorSetup.txnStore;
    }

    @After
    public void tearDown() throws Exception {
        for (Txn id : createdParentTxns) {
            try {
                TxnView txn = txnStore.getTransaction(id.getTxnId());
                if ((txn != null && txn.getEffectiveState().isFinal()) || id.getState().isFinal())
                    continue;
            } catch (TransactionMissing missing) {
                continue;
            }
            id.rollback();
        }
    }

    @Test
    public void testGetActiveWriteTransactionsShowsWriteTransactions() throws Exception{
        Txn parent=control.beginTransaction(DESTINATION_TABLE);
        transactorSetup.timestampSource.rememberTimestamp(parent.getTxnId());
        Set<Long> ids=txnStore.getActiveTransactionIds(parent, DESTINATION_TABLE);
        Assert.assertEquals("Incorrect size",1,ids.size());
        Set<Long> expected = new HashSet<>(1);
        expected.add(parent.getTxnId());
        Assert.assertEquals("Incorrect values", expected, ids);

        TimestampSource timestampSource=transactorSetup.timestampSource;
        timestampSource.nextTimestamp();
        timestampSource.nextTimestamp();


        //now see if a read-only doesn't show
        Txn next=control.beginTransaction();
        ids=txnStore.getActiveTransactionIds(next,DESTINATION_TABLE);
        Assert.assertEquals("Incorrect size",1,ids.size());
        Assert.assertEquals("Incorrect values", expected, ids);
    }

    @Test
    public void testGetActiveTransactionsWorksWithGap() throws Exception{
        Txn parent=control.beginTransaction(DESTINATION_TABLE);

        transactorSetup.timestampSource.rememberTimestamp(parent.getTxnId());
        Set<Long> ids=txnStore.getActiveTransactionIds(parent,null);
        Assert.assertEquals("Incorrect size",1,ids.size());
        Set<Long> expected = new HashSet<>(1);
        expected.add(parent.getTxnId());
        Assert.assertEquals("Incorrect values", expected, ids);

        TimestampSource timestampSource=transactorSetup.timestampSource;
        timestampSource.nextTimestamp();
        timestampSource.nextTimestamp();


        Txn next=control.beginTransaction(DESTINATION_TABLE);
        ids=txnStore.getActiveTransactionIds(next,DESTINATION_TABLE);
        Assert.assertEquals("Incorrect size",2,ids.size());
        expected.add(next.getTxnId());
        Assert.assertEquals("Incorrect values", expected, ids);
    }

    @Test
    public void testGetActiveTransactionsFiltersOutIfParentRollsbackGrandChildCommits() throws Exception{
        Txn parent=new ForwardingTxnView(control.beginTransaction(DESTINATION_TABLE)) {
            @Override
            public boolean allowsSubtransactions() {
                return false;
            }
        };
        transactorSetup.timestampSource.rememberTimestamp(parent.getTxnId());
        Txn child=new ForwardingTxnView(control.beginChildTransaction(parent,parent.getIsolationLevel(),DESTINATION_TABLE)) {
            @Override
            public boolean allowsSubtransactions() {
                return false;
            }
        };
        Txn grandChild=control.beginChildTransaction(child,child.getIsolationLevel(),DESTINATION_TABLE);
        Set<Long> ids=txnStore.getActiveTransactionIds(grandChild,DESTINATION_TABLE);
        Assert.assertEquals("Incorrect size",3,ids.size());
        Set<Long> expected = new HashSet<>(3);
        expected.add(parent.getTxnId());
        expected.add(child.getTxnId());
        expected.add(grandChild.getTxnId());
        Assert.assertEquals("Incorrect values", expected, ids);
        @SuppressWarnings("UnusedDeclaration") Txn grandGrandChild=control.beginChildTransaction(child,child.getIsolationLevel(),DESTINATION_TABLE);

        //commit grandchild, leave child alone, and rollback parent, then see who's active. should be noone
        grandChild.commit();
        parent.rollback();

        Txn next=control.beginTransaction(DESTINATION_TABLE);
        ids=txnStore.getActiveTransactionIds(next,DESTINATION_TABLE);
        Assert.assertEquals("Incorrect size",1,ids.size());
        expected.clear();
        expected.add(next.getTxnId());
        Assert.assertEquals("Incorrect values", expected, ids);
    }

    @Test
    public void oldestActiveTransactionsOne() throws IOException{
        final Txn t1=control.beginTransaction(DESTINATION_TABLE);
        transactorSetup.timestampSource.rememberTimestamp(t1.getTxnId());
        Set<Long> ids=txnStore.getActiveTransactionIds(t1,null);
        Assert.assertEquals(1,ids.size());
        Assert.assertEquals(t1.getTxnId(), (long)ids.iterator().next());
    }

    @Test
    public void oldestActiveTransactionsTwo() throws IOException{
        final Txn t0=control.beginTransaction(DESTINATION_TABLE);
        transactorSetup.timestampSource.rememberTimestamp(t0.getTxnId());
        final Txn t1=control.beginTransaction(DESTINATION_TABLE);
        Set<Long> ids=txnStore.getActiveTransactionIds(t1,DESTINATION_TABLE);
        Assert.assertEquals(2,ids.size());
        Set<Long> expected = new HashSet<>(3);
        expected.add(t0.getTxnId());
        expected.add(t1.getTxnId());
        Assert.assertEquals(expected,ids);
    }

    @Test
    public void oldestActiveTransactionsFuture() throws IOException{
        final Txn t0=control.beginTransaction(DESTINATION_TABLE);
        transactorSetup.timestampSource.rememberTimestamp(t0.getTxnId());
        final Txn t1=control.beginTransaction(DESTINATION_TABLE);
        control.beginTransaction();
        final Set<Long> ids=txnStore.getActiveTransactionIds(t1,DESTINATION_TABLE);
        Assert.assertEquals(2,ids.size());
        Set<Long> expected = new HashSet<>(3);
        expected.add(t0.getTxnId());
        expected.add(t1.getTxnId());
        Assert.assertEquals(expected,ids);
    }

    @Test
    public void oldestActiveTransactionsSkipCommitted() throws IOException{
        final Txn t0=control.beginTransaction(DESTINATION_TABLE);
        transactorSetup.timestampSource.rememberTimestamp(t0.getTxnId());
        final Txn t1=control.beginTransaction(DESTINATION_TABLE);
        final Txn t2=control.beginTransaction(DESTINATION_TABLE);
        t0.commit();
        final Set<Long> ids=txnStore.getActiveTransactionIds(t2,DESTINATION_TABLE);
        Assert.assertEquals(2,ids.size());
        Set<Long> expected = new HashSet<>(3);
        expected.add(t1.getTxnId());
        expected.add(t2.getTxnId());
        Assert.assertEquals(expected,ids);
    }

    @Test
    @Ignore("This is subject to contamination failures when other tests are running concurrently")
    public void oldestActiveTransactionsSkipCommittedGap() throws IOException{
        final Txn t0=control.beginTransaction(DESTINATION_TABLE);
        transactorSetup.timestampSource.rememberTimestamp(t0.getTxnId());
        final Txn t1=control.beginTransaction(DESTINATION_TABLE);
        final Txn t2=control.beginTransaction(DESTINATION_TABLE);
        t1.commit();
        final Set<Long> ids=txnStore.getActiveTransactionIds(t2,DESTINATION_TABLE);
        Assert.assertEquals(2,ids.size());
        Set<Long> expected = new HashSet<>(3);
        expected.add(t0.getTxnId());
        expected.add(t2.getTxnId());
        Assert.assertEquals(expected,ids);
    }

    @Test
    @Ignore("This is subject to contamination failures when other tests are running concurrently")
    public void oldestActiveTransactionsSavedTimestampAdvances() throws IOException{
        final Txn t0=control.beginTransaction(DESTINATION_TABLE);
        transactorSetup.timestampSource.rememberTimestamp(t0.getTxnId()-1);
        Txn t2=control.beginTransaction(DESTINATION_TABLE);
        final Txn t3=control.beginTransaction(DESTINATION_TABLE);
        t0.commit();
        final long originalSavedTimestamp=transactorSetup.timestampSource.retrieveTimestamp();
        transactorSetup.timestampSource.rememberTimestamp(t3.getTxnId()-1);
        txnStore.getActiveTransactionIds(t3,DESTINATION_TABLE);
        final long newSavedTimestamp=transactorSetup.timestampSource.retrieveTimestamp();
        Assert.assertEquals(originalSavedTimestamp+2,newSavedTimestamp);
    }

    @Test
    public void oldestActiveTransactionsDoesNotIgnoreEffectiveStatus() throws IOException {
        final Txn t0 = new ForwardingTxnView(control.beginTransaction(DESTINATION_TABLE)) {
            @Override
            public boolean allowsSubtransactions() {
                return false;
            }
        };
        transactorSetup.timestampSource.rememberTimestamp(t0.getTxnId());
        final Txn t1 = control.beginChildTransaction(t0, DESTINATION_TABLE);
        t1.commit();
        final Txn t2 = control.beginChildTransaction(t0, DESTINATION_TABLE);
        Set<Long> active = txnStore.getActiveTransactionIds(t2, DESTINATION_TABLE);
        Assert.assertEquals(3, active.size());
        Set<Long> expected = new HashSet<>(3);
        expected.add(t0.getTxnId());
        expected.add(t1.getTxnId());
        expected.add(t2.getTxnId());
        Assert.assertEquals(expected, active);
    }

    @Test
    public void oldestActiveTransactionIgnoresCommitTimestampIds() throws IOException{
        final Txn t0=control.beginTransaction(DESTINATION_TABLE);
        transactorSetup.timestampSource.rememberTimestamp(t0.getTxnId());
        LongArrayList committedTxns=new LongArrayList();
        for(int i=0;i<4;i++){
            final Txn transactionId=control.beginTransaction(DESTINATION_TABLE);
            transactionId.commit();
            committedTxns.add(transactionId.getTxnId());
        }

        final Txn t1=control.beginTransaction(DESTINATION_TABLE);
        final Set<Long> ids=txnStore.getActiveTransactionIds(t1,DESTINATION_TABLE);
        Set<Long> expected = new HashSet<>(2);
        expected.add(t0.getTxnId());
        expected.add(t1.getTxnId());
        Assert.assertEquals(2,ids.size());
        Assert.assertEquals(expected, ids);
        //this transaction should still be missing
    }

    @Test
    @Ignore("This is subject to contamination failures when other tests are running concurrently")
    public void oldestActiveTransactionsManyActive() throws IOException{
        Set<Long> startedTxns=new HashSet();
        final Txn t0=control.beginTransaction(DESTINATION_TABLE);
        startedTxns.add(t0.getTxnId());
        transactorSetup.timestampSource.rememberTimestamp(t0.getTxnId());
//        long[] startIds = txnStore.getActiveTransactionIds(t0,DESTINATION_TABLE);
//        for(long sId:startIds){
//            startedTxns.add(sId);
//        }
        for(int i=0;i<4;i++){
            Txn transactionId=control.beginTransaction(DESTINATION_TABLE);
            startedTxns.add(transactionId.getTxnId());
        }
        final Txn t1=control.beginTransaction(DESTINATION_TABLE);
        startedTxns.add(t1.getTxnId());
        final Set<Long> ids=txnStore.getActiveTransactionIds(t0.getTxnId(),t1.getTxnId(),DESTINATION_TABLE);

        Assert.assertEquals(startedTxns.size(),ids.size());
        Assert.assertEquals(startedTxns,ids);
//        Assert.assertEquals(t0.getTxnId(), ids[0]);
//        Assert.assertEquals(t1.getTxnId(), result.get(ids.length - 1).getId());
    }
}
