package com.splicemachine.hbase.locks;

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

import com.splicemachine.utils.SpliceLogUtils;

public class TxnLockManager {
	private static Logger LOG = Logger.getLogger(TxnLockManager.class);

	/**
	 * @author jessiezhang: Using ReadWriteLock, we need to ask ourselves the following questions:
	 * 1. Determining whether to grant the read lock or the write lock, when both readers and writers are waiting, 
	 * at the time that a writer releases the write lock. Writer preference is common, as writes are expected to 
	 * be short and infrequent. Reader preference is less common as it can lead to lengthy delays for a write if 
	 * the readers are frequent and long-lived as expected. Fair, or "in-order" implementations are also possible.
	 * 2. Determining whether readers that request the read lock while a reader is active and a writer is waiting, 
	 * are granted the read lock. Preference to the reader can delay the writer indefinitely, while preference to 
	 * the writer can reduce the potential for concurrency.
	 * 3. Determining whether the locks are reentrant: can a thread with the write lock reacquire it? Can it acquire 
	 * a read lock while holding the write lock? Is the read lock itself reentrant?
	 * 4. Can the write lock be downgraded to a read lock without allowing an intervening writer? Can a read lock 
	 * be upgraded to a write lock, in preference to other waiting readers or writers?
	 * 
	 * ReentrantReadWriteLocks can be used to improve concurrency in some uses of some kinds of Collections. This is 
	 * typically worthwhile only when the collections are expected to be large, accessed by more reader threads than 
	 * writer threads, and entail operations with overhead that outweighs synchronization overhead. 
	 */
	private ConcurrentHashMap<String, ExclusiveLockWorker> threadsByTransactionID = new ConcurrentHashMap<String, ExclusiveLockWorker>();
	private ReentrantReadWriteLock threadMapLock = new ReentrantReadWriteLock(); //protect the threadsByTransactionID
	private ReadLock rtl= threadMapLock.readLock();
	private WriteLock wtl = threadMapLock.writeLock();
	
	private HashMap<String, ReentrantReadWriteLock> locksByRow = new HashMap<String, ReentrantReadWriteLock>();
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
		boolean acquired = false;
		String rowKey = Bytes.toString(row);
		SpliceLogUtils.debug(LOG, "Thread " + Thread.currentThread().getId() + " wait for reading lock map.");
		rml.lock();
		SpliceLogUtils.debug(LOG, "Thread " + Thread.currentThread().getId() + " acquired the read lock of lock map.");
		try {
			if (locksByRow.containsKey(rowKey)) {
				SpliceLogUtils.debug(LOG, "found existing shared lock for " + rowKey);
				if (isWriteLock) {
					WriteLock wl = locksByRow.get(rowKey).writeLock();
					rml.unlock();		
					acquired = wl.tryLock(timeout, TimeUnit.SECONDS);
					SpliceLogUtils.debug(LOG, "acquired shared write lock for " + rowKey);
				} else {
					ReadLock rl = locksByRow.get(rowKey).readLock();
					rml.unlock();
					acquired = rl.tryLock(timeout, TimeUnit.SECONDS);
					SpliceLogUtils.debug(LOG, "acquired shared read lock for " + rowKey);
				}
			} else {
				rml.unlock();
				SpliceLogUtils.debug(LOG, "Thread " + Thread.currentThread().getId() + " unlock the read lock of lock map.");
				wml.lock();
				SpliceLogUtils.debug(LOG, "Thread " + Thread.currentThread().getId() + " lock the write lock of lock map.");
				if (locksByRow.containsKey(rowKey)) { //Double check pattern
					rml.lock();
					wml.unlock();
					SpliceLogUtils.debug(LOG, "found existing shared lock for " + rowKey);
					if (isWriteLock) {
						WriteLock wl = locksByRow.get(rowKey).writeLock();
						rml.unlock();		
						acquired = wl.tryLock(timeout, TimeUnit.SECONDS);
						if (acquired)
							SpliceLogUtils.debug(LOG, "acquired shared write lock for " + rowKey);
					} else {
						ReadLock rl = locksByRow.get(rowKey).readLock();
						rml.unlock();
						acquired = rl.tryLock(timeout, TimeUnit.SECONDS);
						if (acquired)
							SpliceLogUtils.debug(LOG, "acquired shared read lock for " + rowKey);
					}
				} else {
					SpliceLogUtils.debug(LOG, "create new shared lock for " + rowKey);
					ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
					if (isWriteLock) {
						acquired = lock.writeLock().tryLock(timeout, TimeUnit.SECONDS);
						if (acquired)
							SpliceLogUtils.debug(LOG, "acquired shared write lock for " + rowKey);
					} else {
						acquired = lock.readLock().tryLock(timeout, TimeUnit.SECONDS);
						if (acquired)
							SpliceLogUtils.debug(LOG, "acquired shared read lock for " + rowKey);
					}
					if (acquired) 
						locksByRow.put(rowKey, lock);
					
					wml.unlock();
				}
			}
		} finally {
			if (lockMapLock.getReadHoldCount() != 0)
				rml.unlock();
		}
		return acquired;  
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
					SpliceLogUtils.debug(LOG, "release shared write lock for " + rowKey);
					lock.writeLock().unlock();
				}
			} else {
				SpliceLogUtils.debug(LOG, "release shared read lock for " + rowKey);
				lock.readLock().unlock();
			}
			if (!lock.hasQueuedThreads() && lock.getReadLockCount() == 0 && !lock.isWriteLocked()) {
				rml.unlock();
				SpliceLogUtils.debug(LOG, "unlock read lock of lock lock map.");
				wml.lock();
				SpliceLogUtils.debug(LOG, "lock write lock of lock lock map.");
				try {
					if (!lock.hasQueuedThreads() && lock.getReadLockCount() == 0 && !lock.isWriteLocked()) { //Double check pattern
						SpliceLogUtils.debug(LOG, "remove lock from lock map for row " + rowKey);
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
			SpliceLogUtils.debug(LOG, "Thread " + Thread.currentThread().getId() + " Start run with parameter: " + running + " " + acquiring + " " + Bytes.toString(row) + " " + isWriteLock );
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
					SpliceLogUtils.debug(LOG, "Thread " + Thread.currentThread().getId() + " Start running");
					if (acquiring) {
						SpliceLogUtils.debug(LOG, "Thread " + Thread.currentThread().getId() + " acquiring");
						try {
							result = acquireExclusiveLock(txnID, row, isWriteLock, infos);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					} else {
						SpliceLogUtils.debug(LOG, "Thread " + Thread.currentThread().getId() + " releasing txnID " + txnID);
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
				//TODO: use lock to protect thread map.
				try {
					if (!threadsByTransactionID.containsKey(txnID)) { //Double check pattern
						worker = new ExclusiveLockWorker(txnID);
						SpliceLogUtils.debug(LOG, "create new thread " + worker.getId() + " for txnID " + txnID);
						worker.runWithParameter(Thread.currentThread(), true, row, isWriteLock);
						threadsByTransactionID.put(txnID, worker);
					}
				} finally {
					rtl.lock();
					wtl.unlock();
				}
			} else {
				SpliceLogUtils.debug(LOG, "found existing thread for txnID " + txnID);
				worker = threadsByTransactionID.get(txnID);
				worker.runWithParameter(Thread.currentThread(), true, row, isWriteLock);
			}
		} finally {
			rtl.unlock();
			LockSupport.park(this);
			//FIXME: need to fix and uncomment out this 
			//return worker.getResult();
		}
		return false; 
	}

	public void releaseExclusiveLocks(String txnID) {
		SpliceLogUtils.debug(LOG, "release exclusive locks with txnID " + txnID);
		
		if (threadsByTransactionID.get(txnID) == null) {
			SpliceLogUtils.debug(LOG, "threadsByTransactionID does not have txnID="+txnID+" since this transaction does not have exclusive locks");
			/*String key = null;
			for (Iterator<String> it = locksByRow.keySet().iterator(); it.hasNext();) {
				key = it.next();
				if (locksByRow.get(key).isWriteLocked())
					LOG.info("in locksByRow, WriteLock for key="+key);
				else
					LOG.info("in locksByRow, ReadLock for key="+key);
			}*/
			return;
		}
		threadsByTransactionID.get(txnID).runWithParameter(Thread.currentThread(), false, null, null);
		LockSupport.park(this);
	}


	private boolean acquireExclusiveLock(String txnID, byte[] row, boolean isWriteLock, List<LockInfo> infos) throws InterruptedException {
		boolean acquired = false;
		LockInfo info = new LockInfo(row, isWriteLock);
		String rowKey = Bytes.toString(row);
		rml.lock();
		try {
			if (locksByRow.containsKey(rowKey)) {
				SpliceLogUtils.debug(LOG, "found existing lock in lock map for " + rowKey + " with txnID " + txnID);
				if (isWriteLock) {
					SpliceLogUtils.debug(LOG, "acquire exclusive write lock for row " + rowKey + " with txnID " + txnID);
					WriteLock wl = locksByRow.get(rowKey).writeLock();
					rml.unlock();
					//FIXME: Do we think the transaction is executed by the same thread? how about the concurrent cases?
					//We are trying to use this to check whether writeLock has been acquired by another transaction by
					//check whether it's be been held by the current thread?????
					if (!wl.isHeldByCurrentThread())
						acquired = wl.tryLock(timeout, TimeUnit.SECONDS);
					if (acquired)
						SpliceLogUtils.debug(LOG, "Done with acquiring exclusive write lock for row " + rowKey + " with txnID " + txnID);
				} else {
					SpliceLogUtils.debug(LOG, "acquire exclusive read lock for row " + rowKey + " with txnID " + txnID);
					ReentrantReadWriteLock rwlt = locksByRow.get(rowKey);
					rml.unlock();
					
					//FIXME: shouldn't we also check isolation level? if it's read_uncommitted, we should not acquire a lock
					//if not, we should check writeLock as well unless writeLock implies readLock as well
					ReadLock rl = rwlt.readLock();
					if (rwlt.getReadHoldCount() == 0) {
						acquired = rl.tryLock(timeout, TimeUnit.SECONDS);
					}
					if (acquired)
						SpliceLogUtils.debug(LOG, "Done with acquiring exclusive read lock for row " + rowKey + " with txnID " + txnID);
				}
			} else {
				rml.unlock();
				wml.lock();
				if (locksByRow.containsKey(rowKey)) { //Double check pattern
					rml.lock();
					wml.unlock();
					SpliceLogUtils.debug(LOG, "found existing shared lock for " + rowKey);
					if (isWriteLock) {
						WriteLock wl = locksByRow.get(rowKey).writeLock();
						rml.unlock();		
						acquired = wl.tryLock(timeout, TimeUnit.SECONDS);
						if (acquired)
							SpliceLogUtils.debug(LOG, "acquired shared write lock for " + rowKey);
					} else {
						ReadLock rl = locksByRow.get(rowKey).readLock();
						rml.unlock();
						acquired = rl.tryLock(timeout, TimeUnit.SECONDS);
						if (acquired)
							SpliceLogUtils.debug(LOG, "acquired shared read lock for " + rowKey);
					}
				} else {
					ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
					if (LOG.isDebugEnabled())
						LOG.debug("create new lock for " + rowKey + " with txnID " + txnID);
					if (isWriteLock) {
						SpliceLogUtils.debug(LOG, "acquire exclusive write lock for row " + rowKey + " with txnID " + txnID);
						acquired = lock.writeLock().tryLock(timeout, TimeUnit.SECONDS);
						if (acquired)
							SpliceLogUtils.debug(LOG, "Done with acquiring exclusive write lock for row " + rowKey + " with txnID " + txnID);
					} else {
						SpliceLogUtils.debug(LOG, "acquire exclusive read lock for row " + rowKey + " with txnID " + txnID);
						acquired = lock.readLock().tryLock(timeout, TimeUnit.SECONDS);
						if (acquired)
							SpliceLogUtils.debug(LOG, "Done with acquiring exclusive read lock for row " + rowKey + " with txnID " + txnID);
					}
					SpliceLogUtils.debug(LOG, "put exclusive lock for row " + rowKey + " with txnID " + txnID);
					locksByRow.put(rowKey, lock);
					wml.unlock();
				}
			}
			if (!infos.contains(info) && acquired) { // only the thread that corresponds to the transaction id can access the corresponding infos at any single time 
				infos.add(info);
			}
		} finally {
			if (lockMapLock.getReadHoldCount() != 0)
				rml.unlock();
		}
		return acquired;
	}

	private boolean releaseExclusiveLock(LockInfo info) throws InterruptedException {
		String rowKey = Bytes.toString(info.row);
		assert locksByRow.containsKey(rowKey);
		rml.lock();
		try {
			ReentrantReadWriteLock lock = locksByRow.get(rowKey);
			if (info.isWriteLock) {
				if (lock.isWriteLockedByCurrentThread()) {
					SpliceLogUtils.debug(LOG, "release exclusive write lock for " + rowKey);
					lock.writeLock().unlock();
				}
			} else {
				SpliceLogUtils.debug(LOG, "release exclusive read lock for " + rowKey);
				lock.readLock().unlock();
			}
			if (!lock.hasQueuedThreads() && lock.getReadLockCount() == 0 && !lock.isWriteLocked()) {
				rml.unlock();
				wml.lock();
				try {
					if (!lock.hasQueuedThreads() && lock.getReadLockCount() == 0 && !lock.isWriteLocked()) { //Double check pattern
						SpliceLogUtils.debug(LOG, "remove lock from lock map for row " + rowKey);
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