package com.splicemachine.si.txn;

import com.splicemachine.si.LStoreSetup;
import com.splicemachine.si.StoreSetup;
import com.splicemachine.si.TransactorSetup;
import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.impl.Transaction;
import org.junit.Assert;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import java.io.IOException;

public class JtaXAResourceTest {
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
    public void startTest() throws XAException, IOException {
        JtaXAResource resource = new JtaXAResource(transactor);
        SpliceXid xid = new SpliceXid(Bytes.toBytes(1), 1, Bytes.toBytes(1));
        resource.start(xid, 0);
        TransactionId transactionId = resource.getThreadLocalTransactionState();
        Assert.assertNotNull(transactionId);
        Transaction transaction = transactorSetup.transactionStore.getTransaction(transactionId);
        Assert.assertTrue(transaction.getBeginTimestampDirect() >= 0);
        Assert.assertTrue(transaction.isEffectivelyActive());
    }

    @Test
    public void commitTest() throws XAException, IOException {
        JtaXAResource resource = new JtaXAResource(transactor);
        SpliceXid xid = new SpliceXid(Bytes.toBytes(1), 1, Bytes.toBytes(1));
        resource.start(xid, 0);
        TransactionId transactionId = resource.getThreadLocalTransactionState();
        Assert.assertNotNull(transactionId);
        resource.commit(xid, true);
        Transaction transaction = transactorSetup.transactionStore.getTransaction(transactionId);
        Assert.assertTrue(transaction.getBeginTimestampDirect() >= 0);
        Assert.assertTrue(transaction.isCommitted());
        Assert.assertTrue(transaction.getBeginTimestampDirect() < transaction.getCommitTimestamp());
    }

    @Test
    public void endTest() throws XAException {
        JtaXAResource resource = new JtaXAResource(transactor);
        SpliceXid xid = new SpliceXid(Bytes.toBytes(1), 1, Bytes.toBytes(1));
        resource.start(xid, 0);
        resource.commit(xid, true);
        resource.end(xid, 0);
        Assert.assertNull(resource.getThreadLocalTransactionState());
    }

    @Test
    public void forgetTest() throws XAException, IOException {
        JtaXAResource resource = new JtaXAResource(transactor);
        SpliceXid xid = new SpliceXid(Bytes.toBytes(1), 1, Bytes.toBytes(1));
        resource.start(xid, 0);
        TransactionId transactionId = resource.getThreadLocalTransactionState();
        resource.forget(xid);
        Transaction transaction = transactorSetup.transactionStore.getTransaction(transactionId);
        Assert.assertNotNull(transaction);
        Assert.assertTrue(transaction.getBeginTimestampDirect() >= 0);
        Assert.assertTrue(!transaction.isActive() && !transaction.isCommitted());
        Assert.assertNull(transaction.getCommitTimestamp());
    }

    @Test
    public void threadLocalTransactionStateTest() throws XAException {
        JtaXAResource resource = new JtaXAResource(transactor);
        SpliceXid xid = new SpliceXid(Bytes.toBytes(1), 1, Bytes.toBytes(1));
        resource.start(xid, 0);
        Assert.assertNotNull(resource.getThreadLocalTransactionState());
    }

    @Test
    public void getTransactionTimeoutTest() throws XAException {
        JtaXAResource resource = new JtaXAResource(transactor);
        resource.setTransactionTimeout(60);
        Assert.assertEquals(60, resource.getTransactionTimeout());
    }

    @Test
    public void isSameRMTest() throws XAException {
        JtaXAResource resource = new JtaXAResource(transactor);
        JtaXAResource resource2 = new JtaXAResource(transactor);
        Assert.assertTrue(resource.isSameRM(resource2));
    }

    @Test
    public void prepareTest() throws XAException, IOException {
        JtaXAResource resource = new JtaXAResource(transactor);
        SpliceXid xid = new SpliceXid(Bytes.toBytes(1), 1, Bytes.toBytes(1));
        resource.start(xid, 0);
        resource.prepare(xid);
        TransactionId transactionId = resource.getThreadLocalTransactionState();
        Assert.assertNotNull(transactionId);
        Transaction transaction = transactorSetup.transactionStore.getTransaction(transactionId);
        Assert.assertTrue(transaction.getBeginTimestampDirect() >= 0);
        Assert.assertTrue(transaction.isEffectivelyActive());
    }

    @Test
    public void recoverTest() throws XAException {
        JtaXAResource resource = new JtaXAResource(transactor);
        resource.start(new SpliceXid(Bytes.toBytes(1), 1, Bytes.toBytes(1)), 0);
        resource.start(new SpliceXid(Bytes.toBytes(2), 1, Bytes.toBytes(2)), 0);
        Assert.assertEquals(2, resource.recover(0).length);
    }

    @Test
    public void rollbackTest() throws XAException, IOException {
        JtaXAResource resource = new JtaXAResource(transactor);
        SpliceXid xid = new SpliceXid(Bytes.toBytes(1), 1, Bytes.toBytes(1));
        resource.start(xid, 0);
        TransactionId transactionId = resource.getThreadLocalTransactionState();
        resource.rollback(xid);
        Transaction transaction = transactorSetup.transactionStore.getTransaction(transactionId);
        Assert.assertNotNull(transaction);
        Assert.assertTrue(transaction.getBeginTimestampDirect() >= 0);
        Assert.assertTrue(!transaction.isActive() && !transaction.isCommitted());
        Assert.assertNull(transaction.getCommitTimestamp());
    }

    @Test
    public void setTransactionTimeoutTest() throws XAException {
        JtaXAResource resource = new JtaXAResource(transactor);
        resource.setTransactionTimeout(60);
        Assert.assertEquals(60, resource.getTransactionTimeout());
    }

    private class SpliceXid implements Xid {
        private byte[] branchQualifier;
        private int formatID;
        private byte[] globalTransactionID;

        public SpliceXid(byte[] branchQualifier, int formatID, byte[] globalTransactionID) {
            this.branchQualifier = branchQualifier;
            this.formatID = formatID;
            this.globalTransactionID = globalTransactionID;
        }

        @Override
        public byte[] getBranchQualifier() {
            return branchQualifier;
        }

        @Override
        public int getFormatId() {
            return formatID;
        }

        @Override
        public byte[] getGlobalTransactionId() {
            return globalTransactionID;
        }

    }
}

