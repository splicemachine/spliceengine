package com.splicemachine.si.txn;

import com.splicemachine.si.LStoreSetup;
import com.splicemachine.si.StoreSetup;
import com.splicemachine.si.TransactorSetup;
import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.impl.Transaction;
import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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
        transactorSetup = new TransactorSetup(storeSetup, true);
        baseSetUp();
    }

    @AfterClass
    public static void tearDown() throws Exception {
    }

    @Test
    public void beginTransactionTest() throws Exception {
        TransactionId transactionId = transactor.beginTransaction(true, false, false);
        Assert.assertNotNull(transactionId);
        Transaction transaction = transactorSetup.transactionStore.getTransaction(transactionId);
        Assert.assertTrue(transaction.beginTimestamp >= 0);
        Assert.assertTrue(transaction.isEffectivelyActive());
    }

    @Test
    public void doCommitTest() throws Exception {
        TransactionId transactionId = transactor.beginTransaction(true, false, false);
        transactor.commit(transactionId);
        Transaction transaction = transactorSetup.transactionStore.getTransaction(transactionId);
        Assert.assertTrue(transaction.beginTimestamp >= 0);
        Assert.assertTrue(transaction.isCommitted());
        Assert.assertTrue(transaction.beginTimestamp < transaction.commitTimestamp);
    }

    @Test
    public void rollbackTest() throws Exception {
        TransactionId transactionId = transactor.beginTransaction(true, false, false);
        transactor.rollback(transactionId);
        Transaction transaction = transactorSetup.transactionStore.getTransaction(transactionId);
        Assert.assertTrue(transaction.beginTimestamp >= 0);
        Assert.assertTrue(!transaction.isActive() && !transaction.isCommitted());
        Assert.assertNull(transaction.commitTimestamp);
    }

}
