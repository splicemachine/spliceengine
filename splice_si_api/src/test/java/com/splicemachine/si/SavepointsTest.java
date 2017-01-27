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

package com.splicemachine.si;

import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.ForwardingLifecycleManager;
import com.splicemachine.si.impl.SavePointNotFoundException;
import com.splicemachine.si.impl.TransactionImpl;
import com.splicemachine.si.testenv.ArchitectureSpecific;
import com.splicemachine.si.testenv.SITestEnv;
import com.splicemachine.si.testenv.SITestEnvironment;
import com.splicemachine.si.testenv.TestTransactionSetup;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.spark_project.guava.collect.Lists;

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
    public void tearDown() throws Exception{
        for(Txn txn : createdParentTxns){
            txn.rollback(); // rollback the transaction to prevent contamination
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
        Assert.assertEquals("Incorrect size",2,ids.length);
        Assert.assertArrayEquals("Incorrect values",new long[]{parent.getTxnId(), transaction.getTxn().getTxnId()},ids);


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

        // release first, should also commit second
        res = transaction.releaseSavePoint("first", null);

        Assert.assertEquals("Wrong txn stack size", 1, res);

        Assert.assertEquals(Txn.State.COMMITTED, first.getState());
        Assert.assertEquals(Txn.State.COMMITTED, second.getState());
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

}
