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

import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TransactionMissing;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.ForwardingLifecycleManager;
import com.splicemachine.si.impl.SavePointNotFoundException;
import com.splicemachine.si.impl.TransactionImpl;
import com.splicemachine.si.testenv.ArchitectureSpecific;
import com.splicemachine.si.testenv.SITestEnv;
import com.splicemachine.si.testenv.SITestEnvironment;
import com.splicemachine.si.testenv.TestTransactionSetup;
import com.splicemachine.si.testenv.TransactorTestUtility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import splice.com.google.common.collect.Lists;

import java.util.List;

/**
 * Tests around the logic for savepoints.
 *
 */
@Category(ArchitectureSpecific.class)
public class SavepointsTest {

    private static final byte[] DESTINATION_TABLE=Bytes.toBytes("1216");

    private static SITestEnv testEnv;
    private static TestTransactionSetup transactorSetup;

    private TxnLifecycleManager control;
    private final List<Txn> createdParentTxns= Lists.newArrayList();
    private TxnStore txnStore;
    private TransactorTestUtility testUtility;

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
        testUtility = new TransactorTestUtility(true,testEnv, transactorSetup);
        txnStore=transactorSetup.txnStore;
        transactorSetup.timestampSource.rememberTimestamp(transactorSetup.timestampSource.nextTimestamp());
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
    public void testCreateSavepoint() throws Exception{
        Txn parent=control.beginTransaction(DESTINATION_TABLE);
        TransactionImpl transaction = new TransactionImpl("user", parent, false, control);

        int res = transaction.setSavePoint("first", null);

        Assert.assertEquals("Wrong txn stack size", 2, res);

        res = transaction.setSavePoint("second", null);

        Assert.assertEquals("Wrong txn stack size", 3, res);

        res = transaction.rollbackToSavePoint("first", null);

        Assert.assertEquals("Wrong txn stack size", 2, res);
    }


    @Test(expected = SavePointNotFoundException.class)
    public void testSavepointNotFound() throws Exception{
        Txn parent=control.beginTransaction(DESTINATION_TABLE);
        TransactionImpl transaction = new TransactionImpl("user", parent, false, control);

        int res = transaction.setSavePoint("first", null);

        Assert.assertEquals("Wrong txn stack size", 2, res);

        transaction.rollbackToSavePoint("second", null);
    }


    @Test
    public void testSavepointsAreActive() throws Exception{
        Txn parent=control.beginTransaction(DESTINATION_TABLE);
        TransactionImpl transaction = new TransactionImpl("user", parent, false, control);

        int res = transaction.setSavePoint("first", null);

        Assert.assertEquals("Wrong txn stack size", 2, res);

        transaction.elevate(DESTINATION_TABLE);


        Txn parent2=control.beginTransaction();

        long[] ids=txnStore.getActiveTransactionIds(parent2,DESTINATION_TABLE);
        Assert.assertEquals("Incorrect size",1,ids.length);
        Assert.assertArrayEquals("Incorrect values",new long[]{parent.getTxnId()},ids);


        res = transaction.rollbackToSavePoint("first", null);

        Assert.assertEquals("Wrong txn stack size", 2, res);


        ids=txnStore.getActiveTransactionIds(parent2,DESTINATION_TABLE);
        Assert.assertEquals("Incorrect size",1,ids.length);
        Assert.assertArrayEquals("Incorrect values",new long[]{parent.getTxnId()},ids);
    }


    @Test
    public void testReleaseSavepoint() throws Exception{
        Txn parent=control.beginTransaction(DESTINATION_TABLE);
        TransactionImpl transaction = new TransactionImpl("user", parent, false, control);

        int res = transaction.setSavePoint("first", null);

        Assert.assertEquals("Wrong txn stack size", 2, res);
        transaction.elevate(DESTINATION_TABLE);
        TxnView first = transaction.getTxn();

        res = transaction.setSavePoint("second", null);

        Assert.assertEquals("Wrong txn stack size", 3, res);
        transaction.elevate(DESTINATION_TABLE);
        Txn second = transaction.getTxn();

        // release first, the stack should shrink
        res = transaction.releaseSavePoint("first", null);

        Assert.assertEquals("Wrong txn stack size", 1, res);
    }


    @Test
    public void testRollbackSavepoint() throws Exception{
        Txn parent=control.beginTransaction(DESTINATION_TABLE);
        TransactionImpl transaction = new TransactionImpl("user", parent, false, control);

        int res = transaction.setSavePoint("first", null);

        Assert.assertEquals("Wrong txn stack size", 2, res);
        transaction.elevate(DESTINATION_TABLE);
        TxnView first = transaction.getTxn();

        res = transaction.setSavePoint("second", null);

        Assert.assertEquals("Wrong txn stack size", 3, res);
        transaction.elevate(DESTINATION_TABLE);
        Txn second = transaction.getTxn();

        // rollback to first, should rollback second
        res = transaction.rollbackToSavePoint("first", null);

        Assert.assertEquals("Wrong txn stack size", 2, res);

        Assert.assertEquals(Txn.State.ROLLEDBACK, first.getState());
        Assert.assertEquals(Txn.State.ROLLEDBACK, second.getState());
    }

    @Test
    public void testElevateWholeStack() throws Exception{
        Txn parent=control.beginTransaction();
        TransactionImpl transaction = new TransactionImpl("user", parent, false, control);
        Assert.assertFalse(transaction.getTxn().allowsWrites());

        int res = transaction.setSavePoint("first", null);
        Assert.assertEquals("Wrong txn stack size", 2, res);
        Assert.assertFalse(transaction.getTxn().allowsWrites());

        res = transaction.setSavePoint("second", null);
        Assert.assertEquals("Wrong txn stack size", 3, res);
        Assert.assertFalse(transaction.getTxn().allowsWrites());

        transaction.elevate(DESTINATION_TABLE);
        Assert.assertTrue(transaction.getTxn().allowsWrites());

        res = transaction.releaseSavePoint("second", null);
        Assert.assertEquals("Wrong txn stack size", 2, res);
        Assert.assertTrue(transaction.getTxn().allowsWrites());

        res = transaction.releaseSavePoint("first", null);
        Assert.assertEquals("Wrong txn stack size", 1, res);
        Assert.assertTrue(transaction.getTxn().allowsWrites());

        transaction.commit();
    }

    @Test
    public void testSomePersistedTxns() throws Exception{
        Txn parent=control.beginTransaction();
        TransactionImpl transaction = new TransactionImpl("user", parent, false, control);
        transaction.elevate(DESTINATION_TABLE);

        for (int i = 0; i < SIConstants.TRASANCTION_INCREMENT * 3; ++i) {
            int res = transaction.setSavePoint("test" + i, null);
            transaction.elevate(DESTINATION_TABLE);
            Assert.assertEquals("Wrong txn stack size", 2 + i, res);
        }

        Txn older=control.beginTransaction();
        Assert.assertTrue("We lost more timestamps than estimated. Difference = " + (older.getTxnId() - parent.getTxnId()),
                older.getTxnId() <= parent.getTxnId() + 0x400);
        Assert.assertTrue("We didnt have persisted savepoints", older.getTxnId() > parent.getTxnId() + 0x300);

    }

    @Test
    public void testRollbackWithSomePersistedTxns() throws Exception{
        Txn parent=control.beginTransaction();
        TransactionImpl t1 = new TransactionImpl("user", parent, false, control);
        t1.elevate(DESTINATION_TABLE);

        Assert.assertEquals("dan90 absent", testUtility.read(t1.getTxn(), "dan90"));
        testUtility.insertAge(t1.getTxn(), "dan90", 20);
        Assert.assertEquals("dan90 age=20 job=null", testUtility.read(t1.getTxn(), "dan90"));

        t1.setSavePoint("first", null);

        // Create some persisted txns
        for (int i = 0; i < SIConstants.TRASANCTION_INCREMENT * 3; ++i) {
            int res = t1.setSavePoint("test" + i, null);
            t1.elevate(DESTINATION_TABLE);
            Assert.assertEquals("Wrong txn stack size", 3 + i, res);
        }

        testUtility.insertAge(t1.getTxn(), "dan90", 30);
        Assert.assertEquals("dan90 age=30 job=null", testUtility.read(t1.getTxn(), "dan90"));

        // rollback past the persisted txns
        t1.rollbackToSavePoint("first", null);
        t1.elevate(DESTINATION_TABLE);


        testUtility.insertAge(t1.getTxn(), "dan90", 20);
        Assert.assertEquals("dan90 age=20 job=null", testUtility.read(t1.getTxn(), "dan90"));
    }

    @Test
    public void testReleaseUserTxnWithSomePersistedTxns() throws Exception{
        Txn parent=control.beginTransaction();
        TransactionImpl t1 = new TransactionImpl("user", parent, false, control);
        t1.elevate(DESTINATION_TABLE);

        Assert.assertEquals("dan92 absent", testUtility.read(t1.getTxn(), "dan92"));
        testUtility.insertAge(t1.getTxn(), "dan92", 20);
        Assert.assertEquals("dan92 age=20 job=null", testUtility.read(t1.getTxn(), "dan92"));

        t1.setSavePoint("first", null);

        // Create some persisted txns
        for (int i = 0; i < SIConstants.TRASANCTION_INCREMENT * 3; ++i) {
            int res = t1.setSavePoint("test" + i, null);
            t1.elevate(DESTINATION_TABLE);
            Assert.assertTrue("Wrong txn stack size", res <= 4);
            res = t1.releaseSavePoint("test" + i, null);
            Assert.assertTrue("Wrong txn stack size", res <= 3);
        }

        testUtility.insertAge(t1.getTxn(), "dan92", 30);
        Assert.assertEquals("dan92 age=30 job=null", testUtility.read(t1.getTxn(), "dan92"));

        // release past the persisted txns
        t1.releaseSavePoint("first", null);

        // Create some more persisted txns
        for (int i = 0; i < SIConstants.TRASANCTION_INCREMENT * 3; ++i) {
            int res = t1.setSavePoint("test" + i, null);
            t1.elevate(DESTINATION_TABLE);
            Assert.assertTrue("Wrong txn stack size", res <= 4);
            res = t1.releaseSavePoint("test" + i, null);
            Assert.assertTrue("Wrong txn stack size", res <= 3);
        }


        int res = t1.setSavePoint("second", null);
        Assert.assertTrue("Wrong txn stack size", res <= 3);

        t1.elevate(DESTINATION_TABLE);


        Assert.assertEquals("dan92 age=30 job=null", testUtility.read(t1.getTxn(), "dan92"));
        testUtility.insertAge(t1.getTxn(), "dan92", 20);
        Assert.assertEquals("dan92 age=20 job=null", testUtility.read(t1.getTxn(), "dan92"));
    }

}
