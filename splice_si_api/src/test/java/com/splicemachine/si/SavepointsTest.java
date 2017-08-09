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
 */

package com.splicemachine.si;

import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TransactionStore;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.impl.ForwardingLifecycleManager;
import com.splicemachine.si.impl.SavePointNotFoundException;
import com.splicemachine.si.impl.TransactionImpl;
import com.splicemachine.si.testenv.ArchitectureSpecific;
import com.splicemachine.si.testenv.SITestEnv;
import com.splicemachine.si.testenv.SITestEnvironment;
import com.splicemachine.si.testenv.TestTransactionSetup;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.spark_project.guava.collect.Lists;
import java.util.List;

/**
 * Tests around the logic for savepoints.
 *
 */
@Ignore
@Category(ArchitectureSpecific.class)
public class SavepointsTest {

    private static final byte[] DESTINATION_TABLE=Bytes.toBytes("1216");

    private static SITestEnv testEnv;
    private static TestTransactionSetup transactorSetup;

    private TxnLifecycleManager control;
    private final List<Txn> createdParentTxns= Lists.newArrayList();
    private TransactionStore txnStore;

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
        txnStore.rollback(createdParentTxns.toArray(new Txn[createdParentTxns.size()])); // rollback the transaction to prevent contamination
    }

    @Test
    public void testCreateSavepoint() throws Exception{
        Txn parent=control.beginTransaction();
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
        Txn parent=control.beginTransaction();
        TransactionImpl transaction = new TransactionImpl("user", parent, false, control);

        int res = transaction.setSavePoint("first", null);

        Assert.assertEquals("Wrong txn stack size", 2, res);

        transaction.rollbackToSavePoint("second", null);
    }
    /*
    JL-TODO Retest
    @Test
    public void testSavepointsAreActive() throws Exception{
        Txn parent=control.beginTransaction();
        TransactionImpl transaction = new TransactionImpl("user", parent, false, control);

        int res = transaction.setSavePoint("first", null);

        Assert.assertEquals("Wrong txn stack size", 2, res);

        transaction.elevate();


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
    */

    @Test
    public void testReleaseSavepoint() throws Exception{
        Txn parent=control.beginTransaction();
        TransactionImpl transaction = new TransactionImpl("user", parent, false, control);

        int res = transaction.setSavePoint("first", null);

        Assert.assertEquals("Wrong txn stack size", 2, res);
        transaction.elevate();
        Txn first = transaction.getTxn();

        res = transaction.setSavePoint("second", null);

        Assert.assertEquals("Wrong txn stack size", 3, res);
        transaction.elevate();
        Txn second = transaction.getTxn();

        // release first, should also commit second
        res = transaction.releaseSavePoint("first", null);

        Assert.assertEquals("Wrong txn stack size", 1, res);

        Assert.assertTrue(first.isCommitted());
        Assert.assertTrue(second.isCommitted());
    }


    @Test
    public void testRollbackSavepoint() throws Exception{
        Txn parent=control.beginTransaction();
        TransactionImpl transaction = new TransactionImpl("user", parent, false, control);

        int res = transaction.setSavePoint("first", null);

        Assert.assertEquals("Wrong txn stack size", 2, res);
        transaction.elevate();
        Txn first = transaction.getTxn();

        res = transaction.setSavePoint("second", null);

        Assert.assertEquals("Wrong txn stack size", 3, res);
        transaction.elevate();
        Txn second = transaction.getTxn();

        // rollback to first, should rollback second
        res = transaction.rollbackToSavePoint("first", null);

        Assert.assertEquals("Wrong txn stack size", 2, res);

        Assert.assertTrue(first.isRolledback());
        Assert.assertTrue(second.isRolledback());
    }

    @Test
    public void testElevateWholeStack() throws Exception{
        Txn parent=control.beginTransaction();
        TransactionImpl transaction = new TransactionImpl("user", parent, false, control);
        Assert.assertFalse(transaction.getTxn().isPersisted());

        int res = transaction.setSavePoint("first", null);
        Assert.assertEquals("Wrong txn stack size", 2, res);
        Assert.assertFalse(transaction.getTxn().isPersisted());

        res = transaction.setSavePoint("second", null);
        Assert.assertEquals("Wrong txn stack size", 3, res);
        Assert.assertFalse(transaction.getTxn().isPersisted());

        transaction.elevate();
        Assert.assertTrue(transaction.getTxn().isPersisted());

        res = transaction.releaseSavePoint("second", null);
        Assert.assertEquals("Wrong txn stack size", 2, res);
        Assert.assertTrue(transaction.getTxn().isPersisted());

        res = transaction.releaseSavePoint("first", null);
        Assert.assertEquals("Wrong txn stack size", 1, res);
        Assert.assertTrue(transaction.getTxn().isPersisted());

        transaction.commit();
    }

}
