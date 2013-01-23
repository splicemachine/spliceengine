package com.ir.hbase.locks.test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import com.ir.hbase.locks.TxnLockManager;
import com.ir.hbase.txn.test.TestConstants;

public class BaseLockTest extends TestConstants {

	public static TxnLockManager lm;
	public static ExecutorService pool;
	public static final int TIME_OUT = 200;

	public static final byte[] ROW1 = "ROW1".getBytes();
	public static final byte[] ROW2 = "ROW2".getBytes();
	public static final byte[] ROW3 = "ROW3".getBytes();
	public static final byte[] ROW4 = "ROW4".getBytes();

	public static final String id1 = "TxnID-1";
	public static final String id2 = "TxnID-2";
	public static final String id3 = "TxnID-3";

	public byte[] getRowKey(int i) {
		return ("ROW" + i).getBytes();
	}

	public class ShasredLockTestUnit implements Callable<Boolean> {
		private boolean isWriteLock;
		private long holdingTime;
		private boolean result;
		public ShasredLockTestUnit(long holdingTime, boolean isWriteLock) {
			this.isWriteLock = isWriteLock;
			this.holdingTime = holdingTime;
		}
		@Override
		public Boolean call() {
			if (isWriteLock) {
				result = lm.acquireSharedWriteLock(ROW1);
			} else {
				result = lm.acquireSharedReadLock(ROW1);
			}
			System.out.println("Thread " + Thread.currentThread().getId() + " acuqired lock.");
			try {
				System.out.println("Thread " + Thread.currentThread().getId() + " sleeping.");
				Thread.sleep(holdingTime);
				System.out.println("Thread " + Thread.currentThread().getId() + " waked up.");
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				if (isWriteLock) {
					lm.releaseSharedWriteLock(ROW1);
					System.out.println("Thread " + Thread.currentThread().getId() + " released shared write lock.");
				} else {
					lm.releaseSharedReadLock(ROW1);
					System.out.println("Thread " + Thread.currentThread().getId() + " released shared read lock.");
				}
			}
			return result;
		}
	}

	public class AcquireExclusiveLockTestUnit implements Callable<Boolean> {
		private String txnID; 
		private boolean isWriteLock;
		private byte[] rowKey;
		private boolean result;

		public AcquireExclusiveLockTestUnit(String txnID, byte[] rowKey, boolean isWriteLock) {
			this.txnID = txnID;
			this.isWriteLock = isWriteLock;
			this.rowKey = rowKey;
		}

		@Override
		public Boolean call() {
			if (isWriteLock) {
				result = lm.acquireExclusiveWriteLock(rowKey, txnID);
				System.out.println("Thread " + Thread.currentThread().getId() + " acquired exclusive write lock with txn id " + txnID);
			} else {
				result = lm.acquireExclusiveReadLock(rowKey, txnID);
				System.out.println("Thread " + Thread.currentThread().getId() + " acquired exclusive read lock with txn id " + txnID);
			}
			return result;
		}
	}

	public class ReleaseExclusiveLockTestUnit implements Runnable {
		private String txnID; 
		private long holdTime;
		public ReleaseExclusiveLockTestUnit(String txnID, long holdTime) {
			this.txnID = txnID;
			this.holdTime = holdTime;
		}
		@Override
		public void run() {
			try {
				System.out.println("Thread " + Thread.currentThread().getId() + " is sleeping. ");
				Thread.sleep(holdTime);
				System.out.println("Thread " + Thread.currentThread().getId() + " waked up. ");
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				lm.releaseExclusiveLocks(txnID);
				System.out.println("Thread " + Thread.currentThread().getId() + " released exclusive locks for " + txnID);
			}
		}
	}
}
