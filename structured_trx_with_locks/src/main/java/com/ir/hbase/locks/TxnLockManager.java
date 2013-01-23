package com.ir.hbase.locks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class TxnLockManager {
	private static Logger LOG = Logger.getLogger(TxnLockManager.class);

	private HashMap<String, ReentrantReadWriteLock> locksByRow = new HashMap<String, ReentrantReadWriteLock>();
	private ConcurrentHashMap<String, ExclusiveLockWorker> threadsByTransactionID = new ConcurrentHashMap<String, ExclusiveLockWorker>();
	private ReentrantReadWriteLock threadMapLock = new ReentrantReadWriteLock(); //protect the threadsByTransactionID
	private ReadLock rtl= threadMapLock.readLock();
	private WriteLock wtl = threadMapLock.writeLock();
	private ReentrantReadWriteLock lockMapLock = new ReentrantReadWriteLock(); //protect the locksByRow
	private ReadLock rml= lockMapLock.readLock();
	private WriteLock wml = lockMapLock.writeLock();
	
	public long timeout;
	
	public TxnLockManager(long timeout) {
		this.timeout = timeout;
	}

	public boolean acquireExclusiveReadLock(byte[] row, String txnID) {
		return acquireExclusiveLock(txnID, row, false);
	}

	public boolean acquireExclusiveWriteLock(byte[] row, String txnID) {
		return acquireExclusiveLock(txnID, row, true);
	}

	public boolean acquireSharedReadLock(byte[] row) {
		try {
			return acquireSharedLock(row, false);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}

	public boolean acquireSharedWriteLock(byte[] row) {
		try {
			return acquireSharedLock(row, true);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}

	private boolean acquireSharedLock(byte[] row, boolean isWriteLock) throws InterruptedException {
		boolean timeOutCheck = false;
		String rowKey = Bytes.toString(row);
		if (LOG.isDebugEnabled())
			LOG.debug("Thread " + Thread.currentThread().getId() + " wait for reading lock map.");
		rml.lock();
		if (LOG.isDebugEnabled())
			LOG.debug("Thread " + Thread.currentThread().getId() + " acquired the read lock of lock map.");
		try {
			if (locksByRow.containsKey(rowKey)) {
				if (LOG.isDebugEnabled())
					LOG.debug("found existing shared lock for " + rowKey);
				if (isWriteLock) {
					WriteLock wl = locksByRow.get(rowKey).writeLock();
					rml.unlock();		
					timeOutCheck = wl.tryLock(timeout, TimeUnit.SECONDS);
					if (LOG.isDebugEnabled() && timeOutCheck)
						LOG.debug("acquired shared write lock for " + rowKey);
				} else {
					ReadLock rl = locksByRow.get(rowKey).readLock();
					rml.unlock();
					timeOutCheck = rl.tryLock(timeout, TimeUnit.SECONDS);
					if (LOG.isDebugEnabled() && timeOutCheck)
						LOG.debug("acquired shared read lock for " + rowKey);
				}
			} else {
				rml.unlock();
				if (LOG.isDebugEnabled())
					LOG.debug("Thread " + Thread.currentThread().getId() + " unlock the read lock of lock map.");
				wml.lock();
				if (LOG.isDebugEnabled())
					LOG.debug("Thread " + Thread.currentThread().getId() + " lock the write lock of lock map.");
				if (locksByRow.containsKey(rowKey)) { //Double check pattern
					rml.lock();
					wml.unlock();
					if (LOG.isDebugEnabled())
						LOG.debug("found existing shared lock for " + rowKey);
					if (isWriteLock) {
						WriteLock wl = locksByRow.get(rowKey).writeLock();
						rml.unlock();		
						timeOutCheck = wl.tryLock(timeout, TimeUnit.SECONDS);
						if (LOG.isDebugEnabled() && timeOutCheck)
							LOG.debug("acquired shared write lock for " + rowKey);
					} else {
						ReadLock rl = locksByRow.get(rowKey).readLock();
						rml.unlock();
						timeOutCheck = rl.tryLock(timeout, TimeUnit.SECONDS);
						if (LOG.isDebugEnabled() && timeOutCheck)
							LOG.debug("acquired shared read lock for " + rowKey);
					}
				} else {
					if (LOG.isDebugEnabled())
						LOG.debug("create new shared lock for " + rowKey);
					ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
					if (isWriteLock) {
						timeOutCheck = lock.writeLock().tryLock(timeout, TimeUnit.SECONDS);
						if (LOG.isDebugEnabled() && timeOutCheck)
							LOG.debug("acquired shared write lock for " + rowKey);
					} else {
						timeOutCheck = lock.readLock().tryLock(timeout, TimeUnit.SECONDS);
						if (LOG.isDebugEnabled() && timeOutCheck)
							LOG.debug("acquired shared read lock for " + rowKey);
					}
					if (timeOutCheck)
						locksByRow.put(rowKey, lock);
					wml.unlock();
				}
			}
		} finally {
			if (lockMapLock.getReadHoldCount() != 0)
				rml.unlock();
		}
		return timeOutCheck;  
	}

	public void releaseSharedReadLock(byte[] row) {
		try {
			releaseSharedLock(row, false);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void releaseSharedWriteLock(byte[] row) {
		try {
			releaseSharedLock(row, true);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void releaseSharedLock(byte[] row, boolean isWriteLock) throws InterruptedException {
		String rowKey = Bytes.toString(row);
		assert locksByRow.containsKey(rowKey);
		rml.lock();
		try {
			ReentrantReadWriteLock lock = locksByRow.get(rowKey);
			if (isWriteLock) {
				if (lock.isWriteLockedByCurrentThread()) {
					if (LOG.isDebugEnabled())
						LOG.debug("release shared write lock for " + rowKey);
					lock.writeLock().unlock();
				}
			} else {
				if (LOG.isDebugEnabled())
					LOG.debug("release shared read lock for " + rowKey);
				lock.readLock().unlock();
			}
			if (!lock.hasQueuedThreads() && lock.getReadLockCount() == 0 && !lock.isWriteLocked()) {
				rml.unlock();
				if (LOG.isDebugEnabled())
					LOG.debug("unlock read lock of lock lock map.");
				wml.lock();
				if (LOG.isDebugEnabled())
					LOG.debug("lock write lock of lock lock map.");
				try {
					if (!lock.hasQueuedThreads() && lock.getReadLockCount() == 0 && !lock.isWriteLocked()) { //Double check pattern
						if (LOG.isDebugEnabled())
							LOG.debug("remove lock from lock map for row " + rowKey);
						locksByRow.remove(rowKey);
					}
				} finally {
					rml.lock();
					wml.unlock();
				}
			}
		} catch (IllegalMonitorStateException e) {
			
		} finally {
			rml.unlock();
		}
	}
	
	private class ExclusiveLockWorker extends Thread {
		private boolean running;
		private boolean acquiring;
		private byte[] row;
		private Boolean isWriteLock;
		private boolean retire = false;
		private Thread caller;
		private String txnID;
		private boolean result = false;
		private List<LockInfo> infos = new ArrayList<LockInfo>();

		public ExclusiveLockWorker(String txnID) {
			this.running = false;
			this.txnID = txnID;
			this.start();
		}

		public synchronized void runWithParameter(Thread caller, boolean acquiring, byte[] row, Boolean isWriteLock) {
			this.acquiring = acquiring;
			this.row = row;
			this.isWriteLock = isWriteLock;
			this.caller = caller;
			this.running = true;
			if (LOG.isDebugEnabled())
				LOG.debug("Thread " + Thread.currentThread().getId() + " Start run with parameter: " + running + " " + acquiring + " " + Bytes.toString(row) + " " + isWriteLock );
			LockSupport.unpark(this);
		}

		public void retire() {
			retire = true;
		}
		
		public boolean getResult() {
			return result;
		}
		
		@Override
		public void run() {
			while(!retire) {
				LockSupport.park(this);
				if (running) {
					if (LOG.isDebugEnabled())
						LOG.debug("Thread " + Thread.currentThread().getId() + " Start running");
					if (acquiring) {
						if (LOG.isDebugEnabled())
							LOG.debug("Thread " + Thread.currentThread().getId() + " acquiring");
						try {
							result = acquireExclusiveLock(txnID, row, isWriteLock, infos);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					} else {
						if (LOG.isDebugEnabled())
							LOG.debug("Thread " + Thread.currentThread().getId() + " releasing txnID " + txnID);
						List<LockInfo> list = new ArrayList<LockInfo>();
						for (LockInfo info : infos) {
							try {
								if (releaseExclusiveLock(info)) {
									list.add(info);
								}
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
						for (LockInfo key : list) {
							infos.remove(key);
						}
						retire();
						threadsByTransactionID.remove(txnID);
					}
					running = false;
				}
				LockSupport.unpark(caller);
			}
		}
	}

	private boolean acquireExclusiveLock(String txnID, byte[] row, boolean isWriteLock) {
		ExclusiveLockWorker worker;
		rtl.lock();
		try {
			if (!threadsByTransactionID.containsKey(txnID)) {
				rtl.unlock();
				wtl.lock();
				//TODO: gfan use lock to protect thread map.
				try {
					if (!threadsByTransactionID.containsKey(txnID)) { //Double check pattern
						worker = new ExclusiveLockWorker(txnID);
						if (LOG.isDebugEnabled())
							LOG.debug("create new thread " + worker.getId() + " for txnID " + txnID);
						worker.runWithParameter(Thread.currentThread(), true, row, isWriteLock);
						threadsByTransactionID.put(txnID, worker);
					}
				} finally {
					rtl.lock();
					wtl.unlock();
				}
			} else {
				if (LOG.isDebugEnabled())
					LOG.debug("found existing thread for txnID " + txnID);
				worker = threadsByTransactionID.get(txnID);
				worker.runWithParameter(Thread.currentThread(), true, row, isWriteLock);
			}
		} finally {
			rtl.unlock();
			LockSupport.park(this);
//			return worker.getResult();
		}
		return false; 
	}

	public void releaseExclusiveLocks(String txnID) {
		if (LOG.isDebugEnabled())
			LOG.debug("release exclusive locks with txnID " + txnID);
		assert threadsByTransactionID.containsKey(txnID);
		threadsByTransactionID.get(txnID).runWithParameter(Thread.currentThread(), false, null, null);
		LockSupport.park(this);
	}


	private boolean acquireExclusiveLock(String txnID, byte[] row, boolean isWriteLock, List<LockInfo> infos) throws InterruptedException {
		boolean timeOutCheck = false;
		LockInfo info = new LockInfo(row, isWriteLock);
		String rowKey = Bytes.toString(row);
		rml.lock();
		try {
			if (locksByRow.containsKey(rowKey)) {
				if (LOG.isDebugEnabled())
					LOG.debug("found existing lock in lock map for " + rowKey + " with txnID " + txnID);
				if (isWriteLock) {
					if (LOG.isDebugEnabled())
						LOG.debug("acquire exclusive write lock for row " + rowKey + " with txnID " + txnID);
					WriteLock wl = locksByRow.get(rowKey).writeLock();
					rml.unlock();
					if (!wl.isHeldByCurrentThread())
						timeOutCheck = wl.tryLock(timeout, TimeUnit.SECONDS);
					if (LOG.isDebugEnabled() && timeOutCheck)
						LOG.debug("Done with acquiring exclusive write lock for row " + rowKey + " with txnID " + txnID);
				} else {
					if (LOG.isDebugEnabled())
						LOG.debug("acquire exclusive read lock for row " + rowKey + " with txnID " + txnID);
					ReentrantReadWriteLock rwlt = locksByRow.get(rowKey);
					rml.unlock();
					ReadLock rl = rwlt.readLock();
					if (rwlt.getReadHoldCount() == 0) {
						timeOutCheck = rl.tryLock(timeout, TimeUnit.SECONDS);
					}
					if (LOG.isDebugEnabled() && timeOutCheck)
						LOG.debug("Done with acquiring exclusive read lock for row " + rowKey + " with txnID " + txnID);
				}
			} else {
				rml.unlock();
				wml.lock();
				if (locksByRow.containsKey(rowKey)) { //Double check pattern
					rml.lock();
					wml.unlock();
					if (LOG.isDebugEnabled())
						LOG.debug("found existing shared lock for " + rowKey);
					if (isWriteLock) {
						WriteLock wl = locksByRow.get(rowKey).writeLock();
						rml.unlock();		
						timeOutCheck = wl.tryLock(timeout, TimeUnit.SECONDS);
						if (LOG.isDebugEnabled() && timeOutCheck)
							LOG.debug("acquired shared write lock for " + rowKey);
					} else {
						ReadLock rl = locksByRow.get(rowKey).readLock();
						rml.unlock();
						timeOutCheck = rl.tryLock(timeout, TimeUnit.SECONDS);
						if (LOG.isDebugEnabled() && timeOutCheck)
							LOG.debug("acquired shared read lock for " + rowKey);
					}
				} else {
					ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
					if (LOG.isDebugEnabled())
						LOG.debug("create new lock for " + rowKey + " with txnID " + txnID);
					if (isWriteLock) {
						if (LOG.isDebugEnabled())
							LOG.debug("acquire exclusive write lock for row " + rowKey + " with txnID " + txnID);
						timeOutCheck = lock.writeLock().tryLock(timeout, TimeUnit.SECONDS);
						if (LOG.isDebugEnabled() && timeOutCheck)
							LOG.debug("Done with acquiring exclusive write lock for row " + rowKey + " with txnID " + txnID);
					} else {
						if (LOG.isDebugEnabled())
							LOG.debug("acquire exclusive read lock for row " + rowKey + " with txnID " + txnID);
						timeOutCheck = lock.readLock().tryLock(timeout, TimeUnit.SECONDS);
						if (LOG.isDebugEnabled() && timeOutCheck)
							LOG.debug("Done with acquiring exclusive read lock for row " + rowKey + " with txnID " + txnID);
					}
					locksByRow.put(rowKey, lock);
					wml.unlock();
				}
			}
			if (!infos.contains(info) && timeOutCheck) { // only the thread that corresponds to the transaction id can access the corresponding infos at any single time 
				infos.add(info);
			}
		} finally {
			if (lockMapLock.getReadHoldCount() != 0)
				rml.unlock();
		}
		return timeOutCheck;
	}

	private boolean releaseExclusiveLock(LockInfo info) throws InterruptedException {
		String rowKey = Bytes.toString(info.row);
		assert locksByRow.containsKey(rowKey);
		rml.lock();
		try {
			ReentrantReadWriteLock lock = locksByRow.get(rowKey);
			if (info.isWriteLock) {
				if (lock.isWriteLockedByCurrentThread()) {
					if (LOG.isDebugEnabled())
						LOG.debug("release exclusive write lock for " + rowKey);
					lock.writeLock().unlock();
				}
			} else {
				if (LOG.isDebugEnabled())
					LOG.debug("release exclusive read lock for " + rowKey);
				lock.readLock().unlock();
			}
			if (!lock.hasQueuedThreads() && lock.getReadLockCount() == 0 && !lock.isWriteLocked()) {
				rml.unlock();
				wml.lock();
				try {
					if (!lock.hasQueuedThreads() && lock.getReadLockCount() == 0 && !lock.isWriteLocked()) { //Double check pattern
						if (LOG.isDebugEnabled())
							LOG.debug("remove lock from lock map for row " + rowKey);
						locksByRow.remove(rowKey);
						return true;
					}
				} finally {
					rml.lock();
					wml.unlock();
				}
			}
			return false;
		} finally {
			rml.unlock();
		}
	}

	public class LockInfo {

		public boolean isWriteLock;
		public byte[] row;

		public LockInfo(byte[] row, boolean isWriteLock) {
			this.isWriteLock = isWriteLock;
			this.row = row;
		}
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof LockInfo) {
				LockInfo info = ((LockInfo) obj);
				return Bytes.equals(this.row, info.row) && this.isWriteLock == info.isWriteLock;
			}
			return false;
		}
	}
}