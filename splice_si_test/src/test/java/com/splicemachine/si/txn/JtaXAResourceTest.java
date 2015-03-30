package com.splicemachine.si.txn;

import com.splicemachine.si.testsetup.LStoreSetup;
import com.splicemachine.si.testsetup.StoreSetup;
import com.splicemachine.si.testsetup.TestTransactionSetup;
import com.splicemachine.si.api.Transactor;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import java.io.IOException;

@Ignore("was not run by suites")
public class JtaXAResourceTest {
    protected static Transactor transactor;

    static StoreSetup storeSetup;
    static TestTransactionSetup transactorSetup;

    static void baseSetUp() {
        transactor = transactorSetup.transactor;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        storeSetup = new LStoreSetup();
        transactorSetup = new TestTransactionSetup(storeSetup, true);
        baseSetUp();
    }

    @AfterClass
    public static void tearDown() throws Exception {
    }

    @Test
    public void startTest() throws XAException, IOException {
				Assert.fail("IMPLEMENT");
        JtaXAResource resource = new JtaXAResource(transactor,transactorSetup.txnLifecycleManager);
        SpliceXid xid = new SpliceXid(Bytes.toBytes(1), 1, Bytes.toBytes(1));
        resource.start(xid, 0);
//        TransactionId transactionId = resource.getThreadLocalTransactionState();
//        Assert.assertNotNull(transactionId);
//        Transaction transaction = transactorSetup.txnStore.getTransaction(transactionId);
//        Assert.assertTrue(transaction.getBeginTimestamp() >= 0);
//        Assert.assertTrue(transaction.getEffectiveStatus().isActive());
    }

    @Test
    public void commitTest() throws XAException, IOException {
				Assert.fail("IMPLEMENT");
//        JtaXAResource resource = new JtaXAResource(transactor,transactorSetup.control);
//        SpliceXid xid = new SpliceXid(Bytes.toBytes(1), 1, Bytes.toBytes(1));
//        resource.start(xid, 0);
//        TransactionId transactionId = resource.getThreadLocalTransactionState();
//        Assert.assertNotNull(transactionId);
//        resource.commit(xid, true);
//        Transaction transaction = transactorSetup.transactionStore.getTransaction(transactionId);
//        Assert.assertTrue(transaction.getBeginTimestamp() >= 0);
//        Assert.assertTrue(transaction.commitTimestamp != null);
//        Assert.assertTrue(transaction.getBeginTimestamp() < transaction.getEffectiveCommitTimestamp());
    }

    @Test
    public void endTest() throws XAException {
				Assert.fail("IMPLEMENT");
//        JtaXAResource resource = new JtaXAResource(transactor,transactorSetup.control);
//        SpliceXid xid = new SpliceXid(Bytes.toBytes(1), 1, Bytes.toBytes(1));
//        resource.start(xid, 0);
//        resource.commit(xid, true);
//        resource.end(xid, 0);
//        Assert.assertNull(resource.getThreadLocalTransactionState());
    }

    @Test
    public void forgetTest() throws XAException, IOException {
				Assert.fail("IMPLEMENT");
//        JtaXAResource resource = new JtaXAResource(transactor,transactorSetup.control);
//        SpliceXid xid = new SpliceXid(Bytes.toBytes(1), 1, Bytes.toBytes(1));
//        resource.start(xid, 0);
//        TransactionId transactionId = resource.getThreadLocalTransactionState();
//        resource.forget(xid);
//        Transaction transaction = transactorSetup.transactionStore.getTransaction(transactionId);
//        Assert.assertNotNull(transaction);
//        Assert.assertTrue(transaction.getBeginTimestamp() >= 0);
//        Assert.assertTrue(transaction.status.equals(TransactionStatus.ROLLED_BACK));
//        Assert.assertNull(transaction.getEffectiveCommitTimestamp());
    }

    @Test
    public void threadLocalTransactionStateTest() throws XAException {
				Assert.fail("IMPLEMENT");
//        JtaXAResource resource = new JtaXAResource(transactor,transactorSetup.control);
//        SpliceXid xid = new SpliceXid(Bytes.toBytes(1), 1, Bytes.toBytes(1));
//        resource.start(xid, 0);
//        Assert.assertNotNull(resource.getThreadLocalTransactionState());
    }

    @Test
    public void getTransactionTimeoutTest() throws XAException {
				Assert.fail("IMPLEMENT");
//        JtaXAResource resource = new JtaXAResource(transactor,transactorSetup.control);
//        resource.setTransactionTimeout(60);
//        Assert.assertEquals(60, resource.getTransactionTimeout());
    }

    @Test
    public void isSameRMTest() throws XAException {
				Assert.fail("IMPLEMENT");
//        JtaXAResource resource = new JtaXAResource(transactor,transactorSetup.control);
//        JtaXAResource resource2 = new JtaXAResource(transactor,transactorSetup.control);
//        Assert.assertTrue(resource.isSameRM(resource2));
    }

    @Test
    public void prepareTest() throws XAException, IOException {
				Assert.fail("IMPLEMENT");
//        JtaXAResource resource = new JtaXAResource(transactor,transactorSetup.control);
//        SpliceXid xid = new SpliceXid(Bytes.toBytes(1), 1, Bytes.toBytes(1));
//        resource.start(xid, 0);
//        resource.prepare(xid);
//        TransactionId transactionId = resource.getThreadLocalTransactionState();
//        Assert.assertNotNull(transactionId);
//        Transaction transaction = transactorSetup.transactionStore.getTransaction(transactionId);
//        Assert.assertTrue(transaction.getBeginTimestamp() >= 0);
//        Assert.assertTrue(transaction.getEffectiveStatus().isActive());
    }

    @Test
    public void recoverTest() throws XAException {
				Assert.fail("IMPLEMENT");
//        JtaXAResource resource = new JtaXAResource(transactor,transactorSetup.control);
//        resource.start(new SpliceXid(Bytes.toBytes(1), 1, Bytes.toBytes(1)), 0);
//        resource.start(new SpliceXid(Bytes.toBytes(2), 1, Bytes.toBytes(2)), 0);
//        Assert.assertEquals(2, resource.recover(0).length);
    }

    @Test
    public void rollbackTest() throws XAException, IOException {
				Assert.fail("IMPLEMENT");
//        JtaXAResource resource = new JtaXAResource(transactor,transactorSetup.control);
//        SpliceXid xid = new SpliceXid(Bytes.toBytes(1), 1, Bytes.toBytes(1));
//        resource.start(xid, 0);
//        TransactionId transactionId = resource.getThreadLocalTransactionState();
//        resource.rollback(xid);
//        Transaction transaction = transactorSetup.transactionStore.getTransaction(transactionId);
//        Assert.assertNotNull(transaction);
//        Assert.assertTrue(transaction.getBeginTimestamp() >= 0);
//        Assert.assertTrue(transaction.status.equals(TransactionStatus.ROLLED_BACK));
//        Assert.assertNull(transaction.getEffectiveCommitTimestamp());
    }

    @Test
    public void setTransactionTimeoutTest() throws XAException {
				Assert.fail("IMPLEMENT");
//        JtaXAResource resource = new JtaXAResource(transactor,transactorSetup.control);
//        resource.setTransactionTimeout(60);
//        Assert.assertEquals(60, resource.getTransactionTimeout());
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

