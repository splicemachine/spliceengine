package com.splicemachine.si.txn;

import com.splicemachine.si.LStoreSetup;
import com.splicemachine.si.StoreSetup;
import com.splicemachine.si.TestTransactionSetup;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.impl.Transaction;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.si.api.TransactionStatus;
import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TransactionManagerTest {
    protected static Transactor transactor;
		protected static TransactionManager control;

    static StoreSetup storeSetup;
    static TestTransactionSetup transactorSetup;

    static void baseSetUp() {
        transactor = transactorSetup.transactor;
				control = transactorSetup.control;
    }

    @BeforeClass
    public static void setUp() {
        storeSetup = new LStoreSetup();
        transactorSetup = new TestTransactionSetup(storeSetup, true);
        baseSetUp();
    }

    @AfterClass
    public static void tearDown() throws Exception {
    }

    @Test
    public void beginTransactionTest() throws Exception {
        TransactionId transactionId = control.beginTransaction();
        Assert.assertNotNull(transactionId);
        Transaction transaction = transactorSetup.transactionStore.getTransaction(transactionId);
        Assert.assertTrue(transaction.getBeginTimestamp() >= 0);
        Assert.assertTrue(transaction.getEffectiveStatus().isActive());
    }

    @Test
    public void doCommitTest() throws Exception {
        TransactionId transactionId = control.beginTransaction();
        control.commit(transactionId);
        Transaction transaction = transactorSetup.transactionStore.getTransaction(transactionId);
        Assert.assertTrue(transaction.getBeginTimestamp() >= 0);
        Assert.assertTrue(transaction.commitTimestamp != null);
        Assert.assertTrue(transaction.getBeginTimestamp() < transaction.getEffectiveCommitTimestamp());
    }

    @Test
    public void rollbackTest() throws Exception {
        TransactionId transactionId = control.beginTransaction();
        control.rollback(transactionId);
        Transaction transaction = transactorSetup.transactionStore.getTransaction(transactionId);
        Assert.assertTrue(transaction.getBeginTimestamp() >= 0);
        Assert.assertTrue(transaction.status.equals(TransactionStatus.ROLLED_BACK));
        Assert.assertNull(transaction.getEffectiveCommitTimestamp());
    }

}
