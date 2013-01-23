package com.ir.hbase.locks.test;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.ir.hbase.locks.TxnLockManager;

public class LockTimeOutTest extends BaseLockTest {
	
	public static final long testTimeOut = 2; //seconds
	
	@Test
	public void testAcquireSharedReadLockTimeout() throws Exception {
		lm = new TxnLockManager(testTimeOut);
		pool = Executors.newCachedThreadPool();
		pool.submit(new ShasredLockTestUnit(3000, true));
		Future<Boolean> result = pool.submit(new ShasredLockTestUnit(100, false));
		pool.shutdown();
		pool.awaitTermination(TIME_OUT, TimeUnit.SECONDS);
		Assert.assertFalse(result.get());
	}
	
	@Test
	public void testAcquireSharedWriteLockTimeout() throws Exception {
		lm = new TxnLockManager(testTimeOut);
		pool = Executors.newCachedThreadPool();
		pool.submit(new ShasredLockTestUnit(3000, false));
		Future<Boolean> result = pool.submit(new ShasredLockTestUnit(100, true));
		pool.shutdown();
		pool.awaitTermination(TIME_OUT, TimeUnit.SECONDS);
		Assert.assertFalse(result.get());
	}
	
	@Test
	public void testAcquireExclusiveReadLockTimeout() throws Exception {
		lm = new TxnLockManager(testTimeOut);
		pool = Executors.newCachedThreadPool();
		pool.submit(new AcquireExclusiveLockTestUnit(id1, ROW1, true));
		Thread.sleep(100);
		pool.execute(new ReleaseExclusiveLockTestUnit(id1, 3000));
		Future<Boolean> result = pool.submit(new AcquireExclusiveLockTestUnit(id2, ROW1, false));
		pool.shutdown();
		pool.awaitTermination(TIME_OUT, TimeUnit.SECONDS);
		Assert.assertFalse(result.get());
	}
	
	@Test
	public void testAcquireExclusiveWriteLockTimeout() throws Exception {
		lm = new TxnLockManager(testTimeOut);
		pool = Executors.newCachedThreadPool();
		pool.submit(new AcquireExclusiveLockTestUnit(id1, ROW1, true));
		Thread.sleep(100);
		pool.execute(new ReleaseExclusiveLockTestUnit(id1, 3000));
		Future<Boolean> result = pool.submit(new AcquireExclusiveLockTestUnit(id2, ROW1, true));
		pool.shutdown();
		pool.awaitTermination(TIME_OUT, TimeUnit.SECONDS);
		Assert.assertFalse(result.get());
	}
	
	@Test
	public void testReleasePartialExclusiveLocks() throws Exception {
		lm = new TxnLockManager(testTimeOut);
		pool = Executors.newCachedThreadPool();
		pool.submit(new AcquireExclusiveLockTestUnit(id1, ROW1, true));
		Thread.sleep(100);
		pool.execute(new ReleaseExclusiveLockTestUnit(id1, 3000));
		pool.submit(new AcquireExclusiveLockTestUnit(id2, ROW2, true));
		pool.submit(new AcquireExclusiveLockTestUnit(id2, ROW3, true));
		Future<Boolean> result = pool.submit(new AcquireExclusiveLockTestUnit(id2, ROW1, true));
		pool.execute(new ReleaseExclusiveLockTestUnit(id2, 2000));
		pool.shutdown();
		pool.awaitTermination(TIME_OUT, TimeUnit.SECONDS);
		Assert.assertFalse(result.get());
	}
}
