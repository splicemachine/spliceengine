package com.splicemachine.hbase.locks.test;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.splicemachine.hbase.locks.TxnLockManager;

public class ExclusiveLockTest extends BaseLockTest {
	
	@Test
	public void testExclusiveReadLock() throws Exception {
		long startTime = System.currentTimeMillis();
		lm = new TxnLockManager(TRANSACTION_LOCK_TIMEOUT);
		pool = Executors.newCachedThreadPool();
		pool.submit(new AcquireExclusiveLockTestUnit(id1, ROW1, false));
		pool.submit(new AcquireExclusiveLockTestUnit(id1, ROW2, false));
		pool.submit(new AcquireExclusiveLockTestUnit(id2, ROW1, false));
		pool.submit(new AcquireExclusiveLockTestUnit(id3, ROW1, false));
		pool.submit(new AcquireExclusiveLockTestUnit(id2, ROW2, false));
		pool.submit(new AcquireExclusiveLockTestUnit(id3, ROW2, false));
		pool.shutdown();
		pool.awaitTermination(TIME_OUT, TimeUnit.SECONDS);
		Assert.assertTrue(System.currentTimeMillis() - startTime < 500);
	}

	@Test
	public void testExclusiveWriteLock() throws Exception {
		long startTime = System.currentTimeMillis();
		lm = new TxnLockManager(TRANSACTION_LOCK_TIMEOUT);
		pool = Executors.newCachedThreadPool();
		pool.submit(new AcquireExclusiveLockTestUnit(id1, ROW1, true));
		Thread.sleep(100);
		pool.execute(new ReleaseExclusiveLockTestUnit(id1, 1000));
		pool.submit(new AcquireExclusiveLockTestUnit(id2, ROW1, true));
		pool.shutdown();
		pool.awaitTermination(TIME_OUT, TimeUnit.SECONDS);
		Assert.assertTrue(System.currentTimeMillis() - startTime > 1100);
	}
	
	@Test
	public void testExclusiveReadLockBlocking() throws Exception {
		long startTime = System.currentTimeMillis();
		lm = new TxnLockManager(TRANSACTION_LOCK_TIMEOUT);
		pool = Executors.newCachedThreadPool();
		pool.submit(new AcquireExclusiveLockTestUnit(id1, ROW1, false));
		Thread.sleep(100);
		pool.execute(new ReleaseExclusiveLockTestUnit(id1, 1000));
		pool.submit(new AcquireExclusiveLockTestUnit(id2, ROW1, true));
		pool.shutdown();
		pool.awaitTermination(TIME_OUT, TimeUnit.SECONDS);
		Assert.assertTrue(System.currentTimeMillis() - startTime > 1100);
	}
	
	@Test
	public void testExclusiveWriteLockBlocking() throws Exception {
		long startTime = System.currentTimeMillis();
		lm = new TxnLockManager(TRANSACTION_LOCK_TIMEOUT);
		pool = Executors.newCachedThreadPool();
		pool.submit(new AcquireExclusiveLockTestUnit(id1, ROW1, true));
		Thread.sleep(100);
		pool.execute(new ReleaseExclusiveLockTestUnit(id1, 1000));
		pool.submit(new AcquireExclusiveLockTestUnit(id2, ROW1, false));
		pool.submit(new AcquireExclusiveLockTestUnit(id3, ROW1, false));
		pool.shutdown();
		pool.awaitTermination(TIME_OUT, TimeUnit.SECONDS);
		Assert.assertTrue(System.currentTimeMillis() - startTime > 1100);
	}

	@Test
	public void testMultiStatesAcquire() throws Exception {
		lm = new TxnLockManager(TRANSACTION_LOCK_TIMEOUT);
		lm.acquireExclusiveWriteLock(ROW1, id1);
		(new Thread() {
			public void run() {
				lm.acquireExclusiveWriteLock(ROW1, id2);
				assert false;
			};
		}).start();
		lm.acquireExclusiveWriteLock(ROW2, id3);
	}

	@Test
	public void testMultiStatesRelease() throws Exception {
		final long startTime = System.currentTimeMillis();
		lm = new TxnLockManager(TRANSACTION_LOCK_TIMEOUT);
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
