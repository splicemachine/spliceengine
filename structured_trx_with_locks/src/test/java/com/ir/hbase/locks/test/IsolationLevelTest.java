package com.ir.hbase.locks.test;

import org.junit.Assert;
import org.junit.Test;

import com.ir.hbase.locks.TxnLockManager;

public class IsolationLevelTest extends BaseLockTest {
	@Test
	public void testDirtyRead() throws Exception {
		final long startTime = System.currentTimeMillis();
		lm = new TxnLockManager(TRANSACTION_LOCK_TIMEOUT);
		lm.acquireSharedReadLock(ROW1);
		(new Thread() {
			public void run() {
				lm.acquireExclusiveWriteLock(ROW1, id2);
				Assert.assertTrue(System.currentTimeMillis() - startTime > 500);
				try {
					Thread.sleep(500); //the period of transaction state 2 to update ROW1
				} catch (InterruptedException e) {
					e.printStackTrace();
				} 
				lm.releaseExclusiveLocks(id2);
			};
		}).start();
		Thread.sleep(500); //the period of transaction state 1 to read ROW1
		lm.releaseSharedReadLock(ROW1);
		long timeStemp = System.currentTimeMillis();
		lm.acquireSharedReadLock(ROW1); //transaction state 1 try to read ROW1 again
		Assert.assertTrue(System.currentTimeMillis() - timeStemp > 500);
	}
	
	@Test
	public void testNonRepeatableRead() throws Exception {
		final long startTime = System.currentTimeMillis();
		lm = new TxnLockManager(TRANSACTION_LOCK_TIMEOUT);
		lm.acquireExclusiveReadLock(ROW1, id1);
		(new Thread() {
			public void run() {
				lm.acquireExclusiveWriteLock(ROW1, id2);
				Assert.assertTrue(System.currentTimeMillis() - startTime > 500);
				try {
					Thread.sleep(500); //the period of transaction state 2 to update ROW1
				} catch (InterruptedException e) {
					e.printStackTrace();
				} 
				lm.releaseExclusiveLocks(id2);
			};
		}).start();
		Thread.sleep(500); //the period of transaction state 1 to read ROW1
		lm.releaseExclusiveLocks(id1);
		long timeStemp = System.currentTimeMillis();
		lm.acquireExclusiveReadLock(ROW1, id1); //transaction state 1 try to read ROW1 again
		Assert.assertTrue(System.currentTimeMillis() - timeStemp > 500);
	}
	
}
