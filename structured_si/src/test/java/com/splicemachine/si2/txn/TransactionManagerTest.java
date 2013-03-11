package com.splicemachine.si2.txn;

import com.splicemachine.si2.HStoreSetup;
import com.splicemachine.si2.LStoreSetup;
import com.splicemachine.si2.StoreSetup;
import com.splicemachine.si2.TransactorSetup;
import com.splicemachine.si2.data.hbase.TransactorFactory;
import com.splicemachine.si2.si.api.TransactionId;
import com.splicemachine.si2.si.api.Transactor;
import com.splicemachine.si2.si.impl.TransactionStatus;
import com.splicemachine.si2.si.impl.TransactionStruct;
import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TransactionManagerTest {
    protected static TransactionManager tm;
    static final boolean useSimple = true;

    static StoreSetup storeSetup;
    static TransactorSetup transactorSetup;
    static Transactor transactor;

    @BeforeClass
    public static void setUp() {
        storeSetup = new LStoreSetup();
        if (!useSimple) {
            storeSetup = new HStoreSetup();
        }
        transactorSetup = new TransactorSetup(storeSetup);
        transactor = transactorSetup.transactor;
        if (!useSimple) {
            TransactorFactory.setTransactor(transactor);
        }
        try {
            tm = new TransactionManager(transactor);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (storeSetup.getTestCluster() != null) {
            storeSetup.getTestCluster().shutdownMiniCluster();
        }
    }

    @Test
    public void beginTransactionTest() throws Exception {
        TransactionId transactionId = tm.beginTransaction();
        Assert.assertNotNull(transactionId);
        TransactionStruct transaction = transactorSetup.transactionStore.getTransactionStatus(transactionId);
        Assert.assertTrue(transaction.beginTimestamp >= 0);
        Assert.assertEquals(TransactionStatus.ACTIVE, transaction.status);
    }

    @Test
    public void prepareCommitTest() throws Exception {
        TransactionId transactionId = tm.beginTransaction();
        tm.prepareCommit(transactionId);
        Assert.assertNotNull(transactionId);
        TransactionStruct transaction = transactorSetup.transactionStore.getTransactionStatus(transactionId);
        Assert.assertTrue(transaction.beginTimestamp >= 0);
        Assert.assertEquals(TransactionStatus.ACTIVE, transaction.status);
    }

    @Test
    public void doCommitTest() throws Exception {
        TransactionId transactionId = tm.beginTransaction();
        tm.doCommit(transactionId);
        TransactionStruct transaction = transactorSetup.transactionStore.getTransactionStatus(transactionId);
        Assert.assertTrue(transaction.beginTimestamp >= 0);
        Assert.assertEquals(TransactionStatus.COMMITED, transaction.status);
        Assert.assertTrue(transaction.beginTimestamp < transaction.commitTimestamp);
    }

    @Test
    public void tryCommitTest() throws Exception {
        TransactionId transactionId = tm.beginTransaction();
        tm.tryCommit(transactionId);
        TransactionStruct transaction = transactorSetup.transactionStore.getTransactionStatus(transactionId);
        Assert.assertTrue(transaction.beginTimestamp >= 0);
        Assert.assertEquals(TransactionStatus.COMMITED, transaction.status);
        Assert.assertTrue(transaction.beginTimestamp < transaction.commitTimestamp);
    }

    @Test
    public void abortTest() throws Exception {
        TransactionId transactionId = tm.beginTransaction();
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
