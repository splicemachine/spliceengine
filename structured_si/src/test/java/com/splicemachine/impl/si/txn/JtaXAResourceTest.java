package com.splicemachine.impl.si.txn;

import java.io.IOException;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import junit.framework.Assert;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.splicemachine.iapi.txn.TransactionState;

public class JtaXAResourceTest {
	protected static TransactionManagerImpl tm;
	@BeforeClass 
	public static void startup() throws IOException {
		tm = new TransactionManagerImpl();
	}
	
	@AfterClass
	public static void tearDown() {
		
	}
	
	@Test
	public void startTest() throws XAException {
		JtaXAResource resource = new JtaXAResource(tm);
		SpliceXid xid = new SpliceXid(Bytes.toBytes(1),1,Bytes.toBytes(1));
		resource.start(xid, 0);
		Transaction transaction = resource.getThreadLocalTransactionState();
		Assert.assertNotNull(transaction);
		Assert.assertTrue(transaction.getStartTimestamp() >= 0);
		Assert.assertEquals(transaction.getTransactionState(),TransactionState.ACTIVE);
		Transaction readTrans = Transaction.readTransaction(transaction.getStartTimestamp());
		Assert.assertEquals(readTrans, transaction);
	}

	@Test
	public void commitTest() throws XAException {
		JtaXAResource resource = new JtaXAResource(tm);
		SpliceXid xid = new SpliceXid(Bytes.toBytes(1),1,Bytes.toBytes(1));
		resource.start(xid, 0);
		Transaction transaction = resource.getThreadLocalTransactionState();		
		resource.commit(xid, true);
		Transaction readTrans = Transaction.readTransaction(transaction.getStartTimestamp());
		Assert.assertNotNull(readTrans);
		Assert.assertEquals(transaction.getStartTimestamp(),readTrans.getStartTimestamp());
		Assert.assertEquals(readTrans.getTransactionState(),TransactionState.COMMIT);
		Assert.assertTrue(readTrans.getStartTimestamp() < readTrans.getCommitTimestamp());
		
	}

	@Test
	public void endTest() throws XAException {
		JtaXAResource resource = new JtaXAResource(tm);
		SpliceXid xid = new SpliceXid(Bytes.toBytes(1),1,Bytes.toBytes(1));
		resource.start(xid, 0);
		resource.commit(xid, true);
		resource.end(xid, 0);
		Assert.assertNull(resource.getThreadLocalTransactionState());
	}

	@Test
	public void forgetTest() throws XAException {
		JtaXAResource resource = new JtaXAResource(tm);
		SpliceXid xid = new SpliceXid(Bytes.toBytes(1),1,Bytes.toBytes(1));
		resource.start(xid, 0);
		Transaction transaction = resource.getThreadLocalTransactionState();		
		resource.forget(xid);
		Transaction readTrans = Transaction.readTransaction(transaction.getStartTimestamp());
		Assert.assertNotNull(readTrans);
		Assert.assertEquals(transaction.getStartTimestamp(),readTrans.getStartTimestamp());
		Assert.assertEquals(readTrans.getTransactionState(),TransactionState.ABORT);
		Assert.assertNull(readTrans.getCommitTimestamp());
	}

	@Test
	public void threadLocalTransactionStateTest() throws XAException {
		JtaXAResource resource = new JtaXAResource(tm);
		SpliceXid xid = new SpliceXid(Bytes.toBytes(1),1,Bytes.toBytes(1));
		resource.start(xid, 0);
		Assert.assertNotNull(resource.getThreadLocalTransactionState());
	}

	@Test
	public void getTransactionTimeoutTest() throws XAException {
		JtaXAResource resource = new JtaXAResource(tm);
		resource.setTransactionTimeout(60);
		Assert.assertEquals(60, resource.getTransactionTimeout());
	}

	@Test
	public void isSameRMTest() throws XAException {
		JtaXAResource resource = new JtaXAResource(tm);
		JtaXAResource resource2 = new JtaXAResource(tm);
		Assert.assertTrue(resource.isSameRM(resource2));
	}

	@Test
	public void prepareTest() throws XAException {
		JtaXAResource resource = new JtaXAResource(tm);
		SpliceXid xid = new SpliceXid(Bytes.toBytes(1),1,Bytes.toBytes(1));
		resource.start(xid, 0);
		resource.prepare(xid);
		Transaction transaction = resource.getThreadLocalTransactionState();
		Assert.assertNotNull(transaction);
		Assert.assertTrue(transaction.getStartTimestamp() >= 0);
		Assert.assertEquals(transaction.getTransactionState(),TransactionState.ACTIVE);
		Transaction readTrans = Transaction.readTransaction(transaction.getStartTimestamp());
		Assert.assertEquals(readTrans, transaction);
	}

	@Test
	public void recoverTest() throws XAException {
		JtaXAResource resource = new JtaXAResource(tm);
		resource.start(new SpliceXid(Bytes.toBytes(1),1,Bytes.toBytes(1)), 0);
		resource.start(new SpliceXid(Bytes.toBytes(2),1,Bytes.toBytes(2)), 0);
		Assert.assertEquals(2, resource.recover(0).length);
	}

	@Test
	public void rollbackTest() throws XAException {		
		JtaXAResource resource = new JtaXAResource(tm);
		SpliceXid xid = new SpliceXid(Bytes.toBytes(1),1,Bytes.toBytes(1));
		resource.start(xid, 0);
		Transaction transaction = resource.getThreadLocalTransactionState();
		resource.rollback(xid);
		Transaction readTrans = Transaction.readTransaction(transaction.getStartTimestamp());
		Assert.assertNotNull(readTrans);
		Assert.assertEquals(transaction.getStartTimestamp(),readTrans.getStartTimestamp());
		Assert.assertEquals(readTrans.getTransactionState(),TransactionState.ABORT);
		Assert.assertNull(readTrans.getCommitTimestamp());
	}

	@Test
	public void setTransactionTimeoutTest() throws XAException {
		JtaXAResource resource = new JtaXAResource(tm);
		resource.setTransactionTimeout(60);
		Assert.assertEquals(60, resource.getTransactionTimeout());
	}

	private class SpliceXid implements Xid {
		private byte[] branchQualifier;
		private int formatID;
		private byte[] globalTransactionID;
		
		public SpliceXid(byte[] branchQualifier,int formatID,byte[] globalTransactionID) {
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
