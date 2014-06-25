package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongArrayList;
import com.splicemachine.concurrent.LongStripedSynchronizer;
import com.splicemachine.si.api.*;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * In-memory representation of a full Transaction Store.
 *
 * This is useful primarily for testing--that way, we don't need an HBase cluster running to test most of our
 * logic.
 *
 * @author Scott Fines
 * Date: 6/23/14
 */
public class InMemoryTxnStore implements TxnStore {
		private LongStripedSynchronizer<ReadWriteLock> lockStriper;
		private final ConcurrentMap<Long,TxnHolder> txnMap;
		private final TimestampSource commitTsGenerator;
		private TxnLifecycleManager tc;
		private final long txnTimeOutIntervalMs;


		public InMemoryTxnStore(TimestampSource commitTsGenerator,long txnTimeOutIntervalMs) {
				this.txnMap = new ConcurrentHashMap<Long, TxnHolder>();
				this.commitTsGenerator = commitTsGenerator;
				this.txnTimeOutIntervalMs = txnTimeOutIntervalMs;
				this.lockStriper = LongStripedSynchronizer.stripedReadWriteLock(16,false);
		}

		@Override
		public Txn getTransaction(long txnId) throws IOException {
				ReadWriteLock rwlLock = lockStriper.get(txnId);
				Lock rl = rwlLock.readLock();
				rl.lock();
				try{
						TxnHolder txn = txnMap.get(txnId);
						if(txn==null) return null;

						if(isTimedOut(txn))
								return getRolledbackTxn(txnId,txn.txn);
						else return txn.txn;
				}finally{
						rl.unlock();
				}
		}



		@Override public boolean transactionCached(long txnId) { return false; }
		@Override public void cache(Txn toCache) {  }

		@Override
		public void recordNewTransaction(Txn txn) throws IOException {
				ReadWriteLock readWriteLock = lockStriper.get(txn.getTxnId());
				Lock wl = readWriteLock.writeLock();
				wl.lock();
				try{
					TxnHolder txn1 = txnMap.get(txn.getTxnId());
					assert txn1==null: " Transaction "+ txn.getTxnId()+" already existed!";
					txnMap.put(txn.getTxnId(),new TxnHolder(txn));
				}finally{
						wl.unlock();
				}
		}

		@Override
		public void rollback(long txnId) throws IOException {
				ReadWriteLock readWriteLock = lockStriper.get(txnId);
				Lock wl = readWriteLock.writeLock();
				wl.lock();
				try{
						TxnHolder txnHolder = txnMap.get(txnId);
						if(txnHolder==null) return; //no transaction exists

						Txn txn = txnHolder.txn;

						Txn.State state = txn.getState();
						if(state!= Txn.State.ACTIVE) return; //nothing to do if we aren't active
						txnHolder.txn = getRolledbackTxn(txnId, txn);
				}finally{
						wl.unlock();
				}
		}

		protected Txn getRolledbackTxn(long txnId, Txn txn) {
				return new InheritingTxnView(txn.getParentTransaction(),txnId,txn.getBeginTimestamp(),
												txn.getIsolationLevel(),true,txn.isDependent(),
												true,txn.isAdditive(),
												true,txn.allowsWrites(),
												-1l,-1l, Txn.State.ROLLEDBACK);
		}

		@Override
		public long commit(long txnId) throws IOException {
				ReadWriteLock readWriteLock = lockStriper.get(txnId);
				Lock wl = readWriteLock.writeLock();
				wl.lock();
				try{
						TxnHolder txnHolder = txnMap.get(txnId);
						if(txnHolder==null) throw new CannotCommitException(txnId,null);

						Txn txn = txnHolder.txn;
						if(txn.getEffectiveState()== Txn.State.ROLLEDBACK)
								throw new CannotCommitException(txnId, Txn.State.ROLLEDBACK);
						if(isTimedOut(txnHolder))
								throw new CannotCommitException(txnId, Txn.State.ROLLEDBACK);
						long commitTs = commitTsGenerator.nextTimestamp();
						txnHolder.txn = new InheritingTxnView(txn.getParentTransaction(),txnId,txn.getBeginTimestamp(),
										txn.getIsolationLevel(),true,txn.isDependent(),
										true,txn.isAdditive(),
										true,txn.allowsWrites(),
										commitTs,commitTs, Txn.State.COMMITTED);
						return commitTs;
				}finally{
						wl.unlock();
				}
		}

		@Override
		public void timeout(long txnId) throws IOException {
				rollback(txnId);
		}

		@Override
		public void elevateTransaction(Txn txn, byte[] newDestinationTable) throws IOException {
				long txnId = txn.getTxnId();
				ReadWriteLock readWriteLock = lockStriper.get(txnId);
				Lock wl = readWriteLock.writeLock();
				wl.lock();
				try{
						Txn writableTxnCopy = new WritableTxn(txn,tc,newDestinationTable);
						TxnHolder oldTxn = txnMap.get(txnId);
						if(oldTxn==null) {
								txnMap.put(txnId,new TxnHolder(writableTxnCopy));
						} else{
								assert oldTxn.txn.getEffectiveState()== Txn.State.ACTIVE : "Cannot elevate transaction "+ txnId +" because it is not active";
								oldTxn.txn = writableTxnCopy;
						}
				}finally{
						wl.unlock();
				}
		}

		@Override
		public long[] getActiveTransactions(Txn txn, byte[] table) throws IOException {
				if(table==null)
						return getAllActiveTransactions(txn);
				else return findActiveTransactions(txn, table);
		}

		public boolean keepAlive(Txn txn) throws TransactionTimeoutException {
				Lock wl = lockStriper.get(txn.getTxnId()).writeLock();
				wl.lock();
				try{
						TxnHolder txnHolder = txnMap.get(txn.getTxnId());
						if(txnHolder==null) return false; //nothing to keep alive
						if(txnHolder.txn.getEffectiveState()== Txn.State.ACTIVE && isTimedOut(txnHolder))
								throw new TransactionTimeoutException(txn.getTxnId());
						if(txn.getEffectiveState()!= Txn.State.ACTIVE) return false;

						txnHolder.keepAliveTs = System.currentTimeMillis();
						return true;
				}finally{
						wl.unlock();
				}
		}

		protected boolean isTimedOut(TxnHolder txn) {
				return txn.txn.getEffectiveState() == Txn.State.ACTIVE &&
								(System.currentTimeMillis() - txn.keepAliveTs) > txnTimeOutIntervalMs;
		}

		private long[] getAllActiveTransactions(Txn txn) throws IOException {
				long minTimestamp = commitTsGenerator.retrieveTimestamp();
				long maxId;
				if(txn==null)
						maxId = Long.MAX_VALUE;
				else maxId = txn.getTxnId();

				LongArrayList activeTxns = new LongArrayList(txnMap.size());
				for(Map.Entry<Long,TxnHolder> txnEntry:txnMap.entrySet()){
						if(isTimedOut(txnEntry.getValue())) continue;
						Txn value = txnEntry.getValue().txn;
						if(value.getEffectiveState()== Txn.State.ACTIVE
										&& value.getTxnId()<=maxId
										&& value.getTxnId()>=minTimestamp)
								activeTxns.add(txnEntry.getKey());
				}
				return activeTxns.toArray();
		}

		private long[] findActiveTransactions(Txn txn, byte[] table) {
				long minTimestamp = commitTsGenerator.retrieveTimestamp();
				long maxId;
				if(txn==null)
						maxId = Long.MAX_VALUE;
				else maxId = txn.getTxnId();
				LongArrayList activeTxns = new LongArrayList(txnMap.size());
				for(Map.Entry<Long,TxnHolder> txnEntry:txnMap.entrySet()){
						if(isTimedOut(txnEntry.getValue())) continue;
						Txn value = txnEntry.getValue().txn;
						if(value.getEffectiveState()== Txn.State.ACTIVE && value.getTxnId()<=maxId && value.getTxnId()>=minTimestamp){
								Collection<byte[]> destinationTables = value.getDestinationTables();
								if(destinationTables.contains(table)){
										activeTxns.add(txnEntry.getKey());
								}
						}
				}
				return activeTxns.toArray();
		}

		public void setLifecycleManager(TxnLifecycleManager lifecycleManager) {
				this.tc = lifecycleManager;
		}

		private static class TxnHolder{
				private volatile Txn txn;
				private volatile long keepAliveTs;

				private TxnHolder(Txn txn) {
						this.txn = txn;
						this.keepAliveTs = System.currentTimeMillis();
				}
		}
}
