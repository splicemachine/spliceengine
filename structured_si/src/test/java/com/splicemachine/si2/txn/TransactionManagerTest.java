package com.splicemachine.si2.txn;

import com.splicemachine.si2.LStoreSetup;
import com.splicemachine.si2.StoreSetup;
import com.splicemachine.si2.TransactorSetup;
import com.splicemachine.si2.si.api.TransactionId;
import com.splicemachine.si2.si.api.Transactor;
import com.splicemachine.si2.si.impl.TransactionStatus;
import com.splicemachine.si2.si.impl.TransactionStruct;
import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TransactionManagerTest {
    protected static Transactor transactor;

    static StoreSetup storeSetup;
    static TransactorSetup transactorSetup;

    static void baseSetUp() {
        transactor = transactorSetup.transactor;
    }

    @BeforeClass
    public static void setUp() {
        storeSetup = new LStoreSetup();
        transactorSetup = new TransactorSetup(storeSetup);
        baseSetUp();
    }

    @AfterClass
    public static void tearDown() throws Exception {
    }

    @Test
    public void beginTransactionTest() throws Exception {
        TransactionId transactionId = transactor.beginTransaction(true, false, false);
        Assert.assertNotNull(transactionId);
        TransactionStruct transaction = transactorSetup.transactionStore.getTransactionStatus(transactionId);
        Assert.assertTrue(transaction.beginTimestamp >= 0);
        Assert.assertEquals(TransactionStatus.ACTIVE, transaction.status);
    }

    @Test
    public void doCommitTest() throws Exception {
        TransactionId transactionId = transactor.beginTransaction(true, false, false);
        transactor.commit(transactionId);
        TransactionStruct transaction = transactorSetup.transactionStore.getTransactionStatus(transactionId);
        Assert.assertTrue(transaction.beginTimestamp >= 0);
        Assert.assertEquals(TransactionStatus.COMMITED, transaction.status);
        Assert.assertTrue(transaction.beginTimestamp < transaction.commitTimestamp);
    }

    @Test
    public void abortTest() throws Exception {
        TransactionId transactionId = transactor.beginTransaction(true, false, false);
        transactor.abort(transactionId);
        TransactionStruct transaction = transactorSetup.transactionStore.getTransactionStatus(transactionId);
        Assert.assertTrue(transaction.beginTimestamp >= 0);
        Assert.assertEquals(TransactionStatus.ABORT, transaction.status);
        Assert.assertNull(transaction.commitTimestamp);
    }

}
