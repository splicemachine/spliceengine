package com.splicemachine.hbase.locks.test;

import org.junit.Assert;
import org.junit.Test;

import com.splicemachine.hbase.locks.TxnLockManager;

public class ReentrantLockTest extends BaseLockTest {
	@Test
	public void testReenterReadLock() throws Exception {
		final long startTime = System.currentTimeMillis();
		lm = new TxnLockManager(TRANSACTION_LOCK_TIMEOUT);
		lm.acquireExclusiveReadLock(ROW1, id1);
		lm.acquireExclusiveReadLock(ROW1, id1);
		(new Thread() {
			public void run() {
				lm.acquireExclusiveWriteLock(ROW1, id2);
				Assert.assertTrue(System.currentTimeMillis() - startTime > 1000);
			};
		}).start();
		Thread.sleep(1000);
		lm.releaseExclusiveLocks(id1);
	}

	@Test
	public void testReenterWriteLock() throws Exception {
		final long startTime = System.currentTimeMillis();
		lm = new TxnLockManager(TRANSACTION_LOCK_TIMEOUT);
		lm.acquireExclusiveWriteLock(ROW1, id1);
		lm.acquireExclusiveWriteLock(ROW1, id1);
		(new Thread() {
			public void run() {
				lm.acquireExclusiveWriteLock(ROW1, id2);
				Assert.assertTrue(System.currentTimeMillis() - startTime > 1000);
			};
		}).start();
		Thread.sleep(1000);
		lm.releaseExclusiveLocks(id1);
	}
}