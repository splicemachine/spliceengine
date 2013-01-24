package com.splicemachine.hbase.locks.test;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.splicemachine.hbase.locks.TxnLockManager;

public class SharedLockTest extends BaseLockTest {

	@Test
	public void testSharedReadLock() throws Exception {
		long startTime = System.currentTimeMillis();
		lm = new TxnLockManager(TRANSACTION_LOCK_TIMEOUT);
		pool = Executors.newCachedThreadPool();
		pool.submit(new ShasredLockTestUnit(2000, false));
		pool.submit(new ShasredLockTestUnit(1000, false));
		pool.submit(new ShasredLockTestUnit(500, false));
		pool.submit(new ShasredLockTestUnit(100, false));
		pool.shutdown();
		pool.awaitTermination(TIME_OUT, TimeUnit.SECONDS);
		Assert.assertTrue(System.currentTimeMillis() - startTime < 3000);
	}

	@Test
	public void testSharedWriteLock() throws Exception {
		long startTime = System.currentTimeMillis();
		lm = new TxnLockManager(TRANSACTION_LOCK_TIMEOUT);
		pool = Executors.newCachedThreadPool();
		pool.submit(new ShasredLockTestUnit(2000, true));
		pool.submit(new ShasredLockTestUnit(1000, true));
		pool.submit(new ShasredLockTestUnit(500, true));
		pool.submit(new ShasredLockTestUnit(100, true));
		pool.shutdown();
		pool.awaitTermination(TIME_OUT, TimeUnit.SECONDS);
		Assert.assertTrue(System.currentTimeMillis() - startTime > 3600);
	}

	@Test
	public void testSharedReadLockBlocking() throws Exception {
		long startTime = System.currentTimeMillis();
		lm = new TxnLockManager(TRANSACTION_LOCK_TIMEOUT);
		pool = Executors.newCachedThreadPool();
		pool.submit(new ShasredLockTestUnit(2000, false));
		pool.submit(new ShasredLockTestUnit(100, true));
		pool.shutdown();
		pool.awaitTermination(TIME_OUT, TimeUnit.SECONDS);
		Assert.assertTrue(System.currentTimeMillis() - startTime > 2000);
	}

	@Test
	public void testSharedWriteLockBlocking() throws Exception {
		long startTime = System.currentTimeMillis();
		lm = new TxnLockManager(TRANSACTION_LOCK_TIMEOUT);
		pool = Executors.newCachedThreadPool();
		pool.submit(new ShasredLockTestUnit(2000, true));
		pool.submit(new ShasredLockTestUnit(100, false));
		pool.submit(new ShasredLockTestUnit(1000, false));
		pool.submit(new ShasredLockTestUnit(100, false));
		pool.shutdown();
		pool.awaitTermination(TIME_OUT, TimeUnit.SECONDS);
		Assert.assertTrue(System.currentTimeMillis() - startTime > 3000);
	}	
}