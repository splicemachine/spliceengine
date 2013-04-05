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
    protected static TransactionManager tm;

    static StoreSetup storeSetup;
    static TransactorSetup transactorSetup;
    static Transactor transactor;

    static void baseSetUp() {
        transactor = transactorSetup.transactor;
        try {
            tm = new TransactionManager(transactor);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        TransactionId transactionId = tm.beginTransaction(true, false, false, null);
        Assert.assertNotNull(transactionId);
        TransactionStruct transaction = transactorSetup.transactionStore.getTransactionStatus(transactionId);
        Assert.assertTrue(transaction.beginTimestamp >= 0);
        Assert.assertEquals(TransactionStatus.ACTIVE, transaction.status);
    }

    @Test
    public void prepareCommitTest() throws Exception {
        TransactionId transactionId = tm.beginTransaction(true, false, false, null);
        tm.prepareCommit(transactionId);
        Assert.assertNotNull(transactionId);
        TransactionStruct transaction = transactorSetup.transactionStore.getTransactionStatus(transactionId);
        Assert.assertTrue(transaction.beginTimestamp >= 0);
        Assert.assertEquals(TransactionStatus.ACTIVE, transaction.status);
    }

    @Test
    public void doCommitTest() throws Exception {
        TransactionId transactionId = tm.beginTransaction(true, false, false, null);
        tm.doCommit(transactionId);
        TransactionStruct transaction = transactorSetup.transactionStore.getTransactionStatus(transactionId);
        Assert.assertTrue(transaction.beginTimestamp >= 0);
        Assert.assertEquals(TransactionStatus.COMMITED, transaction.status);
        Assert.assertTrue(transaction.beginTimestamp < transaction.commitTimestamp);
    }

    @Test
    public void tryCommitTest() throws Exception {
        TransactionId transactionId = tm.beginTransaction(true, false, false, null);
        tm.tryCommit(transactionId);
        TransactionStruct transaction = transactorSetup.transactionStore.getTransactionStatus(transactionId);
        Assert.assertTrue(transaction.beginTimestamp >= 0);
        Assert.assertEquals(TransactionStatus.COMMITED, transaction.status);
        Assert.assertTrue(transaction.beginTimestamp < transaction.commitTimestamp);
    }

    @Test
    public void abortTest() throws Exception {
        TransactionId transactionId = tm.beginTransaction(true, false, false, null);
        tm.abort(transactionId);
        TransactionStruct transaction = transactorSetup.transactionStore.getTransactionStatus(transactionId);
        Assert.assertTrue(transaction.beginTimestamp >= 0);
        Assert.assertEquals(TransactionStatus.ABORT, transaction.status);
        Assert.assertNull(transaction.commitTimestamp);
    }

    @Test
    public void getXAResourceTest() throws Exception {
        Assert.assertNotNull(tm.getXAResource());
    }
}
