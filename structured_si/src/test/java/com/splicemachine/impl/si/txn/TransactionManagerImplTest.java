package com.splicemachine.impl.si.txn;

import java.io.IOException;

import org.junit.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import com.splicemachine.iapi.txn.TransactionState;

public class TransactionManagerImplTest {
	protected static TransactionManagerImpl tm;
	@BeforeClass 
	public static void startup() throws IOException {
		tm = new TransactionManagerImpl();
	}
	
	@AfterClass
	public static void tearDown() {
		
	}
	
	@Test
	public void beginTransactionTest() throws Exception {
		Transaction transaction = tm.beginTransaction();
		Assert.assertNotNull(transaction);
		Assert.assertTrue(transaction.getStartTimestamp() >= 0);
		Assert.assertEquals(transaction.getTransactionState(),TransactionState.ACTIVE);
		Transaction readTrans = Transaction.readTransaction(transaction.getStartTimestamp());
		Assert.assertEquals(readTrans, transaction);
	}

	@Test
	public void prepareCommitTest() throws Exception {
		Transaction transaction = tm.beginTransaction();
		tm.prepareCommit(transaction);
		Assert.assertNotNull(transaction);
		Assert.assertTrue(transaction.getStartTimestamp() >= 0);
		Assert.assertEquals(transaction.getTransactionState(),TransactionState.ACTIVE);
		Transaction readTrans = Transaction.readTransaction(transaction.getStartTimestamp());
		Assert.assertEquals(readTrans, transaction);
	}

	@Test
	public void doCommitTest() throws Exception {
		Transaction transaction = tm.beginTransaction();
		tm.doCommit(transaction);
		Transaction readTrans = Transaction.readTransaction(transaction.getStartTimestamp());
		Assert.assertNotNull(readTrans);
		Assert.assertEquals(transaction.getStartTimestamp(),readTrans.getStartTimestamp());
		Assert.assertEquals(readTrans.getTransactionState(),TransactionState.COMMIT);
		Assert.assertTrue(readTrans.getStartTimestamp() < readTrans.getCommitTimestamp());
	}
	
	@Test
	public void tryCommitTest() throws Exception {
		Transaction transaction = tm.beginTransaction();
		tm.tryCommit(transaction);
		Transaction readTrans = Transaction.readTransaction(transaction.getStartTimestamp());
		Assert.assertNotNull(readTrans);
		Assert.assertEquals(transaction.getStartTimestamp(),readTrans.getStartTimestamp());
		Assert.assertEquals(readTrans.getTransactionState(),TransactionState.COMMIT);
		Assert.assertTrue(readTrans.getStartTimestamp() < readTrans.getCommitTimestamp());
	}
	@Test
	public void abortTest() throws Exception {
		Transaction transaction = tm.beginTransaction();
		tm.abort(transaction);
		Transaction readTrans = Transaction.readTransaction(transaction.getStartTimestamp());
		Assert.assertNotNull(readTrans);
		Assert.assertEquals(transaction.getStartTimestamp(),readTrans.getStartTimestamp());
		Assert.assertEquals(readTrans.getTransactionState(),TransactionState.ABORT);
		Assert.assertNull(readTrans.getCommitTimestamp());
	}
	
	@Test
	public void getXAResourceTest() throws Exception {
		Assert.assertNotNull(tm.getXAResource());
	}
}
