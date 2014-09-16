package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.splicemachine.concurrent.LongStripedSynchronizer;
import com.splicemachine.si.api.*;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;
import java.util.*;
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

		@Override
		public Txn getTransaction(long txnId, boolean getDestinationTables) throws IOException {
				return getTransaction(txnId);
		}


		@Override public boolean transactionCached(long txnId) { return false; }
		@Override public void cache(TxnView toCache) {  }

    @Override public Txn getTransactionFromCache(long txnId) {
        try {
            return getTransaction(txnId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

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

		protected Txn getRolledbackTxn(long txnId, final Txn txn) {
        return new AbstractTxn(txnId,txn.getBeginTimestamp(),txn.getIsolationLevel()) {

            @Override
            public void commit() throws IOException {
                throw new UnsupportedOperationException("Txn is rolled back");
            }

            @Override public void rollback() throws IOException { }

            @Override
            public Txn elevateToWritable(byte[] writeTable) throws IOException {
                throw new UnsupportedOperationException("Txn is rolled back");
            }
            @Override public long getCommitTimestamp() { return -1l; }
            @Override public long getEffectiveCommitTimestamp() { return -1l; }
            @Override public State getEffectiveState() { return State.ROLLEDBACK; }
            @Override public State getState() { return State.ROLLEDBACK; }
        };
		}

		@Override
		public long commit(long txnId) throws IOException {
				ReadWriteLock readWriteLock = lockStriper.get(txnId);
				Lock wl = readWriteLock.writeLock();
				wl.lock();
				try{
						TxnHolder txnHolder = txnMap.get(txnId);
						if(txnHolder==null) throw new CannotCommitException(txnId,null);

						final Txn txn = txnHolder.txn;
						if(txn.getEffectiveState()== Txn.State.ROLLEDBACK)
								throw new CannotCommitException(txnId, Txn.State.ROLLEDBACK);
						if(isTimedOut(txnHolder))
								throw new CannotCommitException(txnId, Txn.State.ROLLEDBACK);

						final long commitTs = commitTsGenerator.nextTimestamp();

						TxnView parentTransaction = txn.getParentTxnView();
						final long globalCommitTs;
						if(parentTransaction==null||parentTransaction.equals(Txn.ROOT_TRANSACTION))
								globalCommitTs = commitTs;
						else{
								//see if the parent has committed yet
								if(parentTransaction.getEffectiveState()== Txn.State.COMMITTED){
										globalCommitTs = parentTransaction.getEffectiveCommitTimestamp();
								}else
										globalCommitTs = -1l;
						}
            txnHolder.txn = new AbstractTxn(txn.getTxnId(),txn.getBeginTimestamp(),txn.getIsolationLevel()) {

                @Override public void commit() throws IOException {  } //do nothing

                @Override
                public void rollback() throws IOException {
                    throw new UnsupportedOperationException("Cannot rollback a committed transaction");
                }

                @Override
                public Txn elevateToWritable(byte[] writeTable) throws IOException {
                    throw new UnsupportedOperationException("Txn is committed");
                }

                @Override
                public long getCommitTimestamp() {
                    return commitTs;
                }

                @Override
                public long getEffectiveCommitTimestamp() {
                    if(txn.isDependent()){
                        return getParentTxnView().getEffectiveCommitTimestamp();
                    }else return commitTs;
                }

                @Override
                public long getGlobalCommitTimestamp() {
                    return globalCommitTs;
                }

                @Override public State getState() { return State.ROLLEDBACK; }
            };
						return commitTs;
				}finally{
						wl.unlock();
				}
		}

		@Override
		public boolean keepAlive(long txnId) throws IOException {
				Lock writeLock = lockStriper.get(txnId).writeLock();
				writeLock.lock();
				try{
						TxnHolder holder = txnMap.get(txnId);
						if(holder==null) return false;

						Txn txn = holder.txn;
						if(txn.getState()!= Txn.State.ACTIVE)
								return false; //don't keep keepAlives going if the transaction is finished
						if(isTimedOut(holder))
								throw new TransactionTimeoutException(txnId);

						holder.keepAliveTs = System.currentTimeMillis();
						return true;
				}finally{
						writeLock.unlock();
				}
		}

		//		@Override
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
		public long[] getActiveTransactionIds(Txn txn, byte[] table) throws IOException {
				long minTs = this.commitTsGenerator.retrieveTimestamp();
				return getActiveTransactionIds(minTs, txn.getTxnId(), table);
		}

		@Override
		public long[] getActiveTransactionIds(long minTxnId, long maxTxnId, byte[] table) throws IOException {
				if(table==null)
						return getAllActiveTransactions(minTxnId,maxTxnId);
				else
						return findActiveTransactions(minTxnId, maxTxnId, table);
		}

    @Override
    public List<TxnView> getActiveTransactions(long minTxnid, long maxTxnId, byte[] table) throws IOException {
        List<TxnView> txns = Lists.newArrayListWithExpectedSize(txnMap.size());
        for(Map.Entry<Long,TxnHolder> txnEntry:txnMap.entrySet()){
            if(isTimedOut(txnEntry.getValue())) continue;
            Txn value = txnEntry.getValue().txn;
            if(value.getEffectiveState()== Txn.State.ACTIVE
                    && value.getTxnId()<=maxTxnId
                    && value.getTxnId()>=minTxnid)
                txns.add(value);
        }
        Collections.sort(txns,new Comparator<TxnView>() {
            @Override
            public int compare(TxnView o1, TxnView o2) {
                if(o1==null){
                    if(o2==null) return 0;
                    return -1;
                }else if(o2==null) return 1;
                return Longs.compare(o1.getTxnId(), o2.getTxnId());
            }
        });
        return txns;
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

		private long[] getAllActiveTransactions(long minTimestamp,long maxId) throws IOException {

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

    private long[] findActiveTransactions(long minTimestamp, long maxId, byte[] table) {
        LongArrayList activeTxns = new LongArrayList(txnMap.size());
        for(Map.Entry<Long,TxnHolder> txnEntry:txnMap.entrySet()){
            if(isTimedOut(txnEntry.getValue())) continue;
            Txn value = txnEntry.getValue().txn;
            if(value.getEffectiveState()== Txn.State.ACTIVE && value.getTxnId()<=maxId && value.getTxnId()>=minTimestamp){
                Iterator<ByteSlice> destinationTables = value.getDestinationTables();
                while(destinationTables.hasNext()){
                    ByteSlice data = destinationTables.next();
                    if(data.equals(table,0,table.length))
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
