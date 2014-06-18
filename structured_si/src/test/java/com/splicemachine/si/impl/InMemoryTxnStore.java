package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongArrayList;
import com.splicemachine.si.api.*;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
		private final ConcurrentMap<Long,Txn> txnMap;
		private final TimestampSource commitTsGenerator;
		private TxnLifecycleManager tc;

		public InMemoryTxnStore(TimestampSource commitTsGenerator) {
				this.txnMap = new ConcurrentHashMap<Long, Txn>();
				this.commitTsGenerator = commitTsGenerator;
		}

		@Override
		public Txn getTransaction(long txnId) throws IOException {
				Txn txn = txnMap.get(txnId);
//				assert txn !=null : "txnId "+ txnId+" does not exist!";

				return txn;
		}

		@Override public boolean transactionCached(long txnId) { return false; }
		@Override public void cache(Txn toCache) {  }

		@Override
		public void recordNewTransaction(Txn txn) throws IOException {
				Txn txn1 = txnMap.putIfAbsent(txn.getTxnId(), txn);
				assert txn1 == null : "Transaction already existed!";
		}

		@Override
		public void rollback(long txnId) throws IOException {
				boolean shouldContinue;
				do{
						Txn txn = txnMap.get(txnId);
						if(txn==null) return; //no transaction exists

						Txn.State state = txn.getState();
						switch(state){
								case ROLLEDBACK:
								case COMMITTED:
										return;
						}
						Txn replacementTxn = new InheritingTxnView(txn.getParentTransaction(),txnId,txn.getBeginTimestamp(),
										txn.getIsolationLevel(),true,txn.isDependent(),
										true,txn.isAdditive(),
										true,txn.allowsWrites(),
										-1l,-1l, Txn.State.ROLLEDBACK);
						shouldContinue = !txnMap.replace(txnId,txn,replacementTxn);
				}while(shouldContinue);
		}

		@Override
		public long commit(long txnId) throws IOException {
				boolean shouldContinue;
				long commitTs;
				do{
						Txn txn = txnMap.get(txnId);
						if(txn==null) throw new CannotCommitException(txnId,null);

						Txn.State state = txn.getState();
						switch(state){
								case ROLLEDBACK:
										throw new CannotCommitException(txnId, Txn.State.ROLLEDBACK);
						}
						commitTs = commitTsGenerator.nextTimestamp();
						Txn replacementTxn = new InheritingTxnView(txn.getParentTransaction(),txnId,txn.getBeginTimestamp(),
										txn.getIsolationLevel(),true,txn.isDependent(),
										true,txn.isAdditive(),
										true,txn.allowsWrites(),
										commitTs,commitTs, Txn.State.COMMITTED);
						shouldContinue = !txnMap.replace(txnId,txn,replacementTxn);
				}while(shouldContinue);
				return commitTs;
		}

		@Override
		public void timeout(long txnId) throws IOException {
				rollback(txnId);
		}

		@Override
		public void elevateTransaction(Txn txn, byte[] newDestinationTable) throws IOException {
				boolean shouldContinue;
				long txnId = txn.getTxnId();
				Txn writableTxnCopy = new WritableTxn(txn,tc,newDestinationTable);
				do{
						Txn oldTxn = txnMap.get(txnId);
						if(oldTxn==null) {
								Txn txn1 = txnMap.putIfAbsent(txnId, writableTxnCopy);
								shouldContinue = txn1!=null;
						}
						else{
								assert  oldTxn.getState()== Txn.State.ACTIVE : "Cannot elevate transaction "+ txnId +" because it is not active";
								shouldContinue = !txnMap.replace(txnId,oldTxn,writableTxnCopy);
						}
				}while(shouldContinue);

		}

		@Override
		public long[] getActiveTransactions(Txn txn, byte[] table) throws IOException {
				if(table==null)
						return getAllActiveTransactions(txn);
				else return findActiveTransactions(txn, table);
		}


		private long[] getAllActiveTransactions(Txn txn) throws IOException {
				long minTimestamp = commitTsGenerator.retrieveTimestamp();
				long maxId;
				if(txn==null)
						maxId = Long.MAX_VALUE;
				else maxId = txn.getTxnId();

				LongArrayList activeTxns = new LongArrayList(txnMap.size());
				for(Map.Entry<Long,Txn> txnEntry:txnMap.entrySet()){
						Txn value = txnEntry.getValue();
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
				for(Map.Entry<Long,Txn> txnEntry:txnMap.entrySet()){
						Txn value = txnEntry.getValue();
						if(value.getEffectiveState()== Txn.State.ACTIVE && value.getTxnId()<=maxId && value.getTxnId()>=minTimestamp){
								Collection<byte[]> destinationTables = value.getDestinationTables();
								if(destinationTables.contains(table)){
										activeTxns.add(txnEntry.getKey());
								}
						}
				}
				return activeTxns.toArray();
		}

		@Override
		public void keepAlive(Txn txn) throws IOException {
				throw new UnsupportedOperationException("IMPLEMENT");
		}

		public void setLifecycleManager(TxnLifecycleManager lifecycleManager) {
				this.tc = lifecycleManager;
		}

}
