package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.api.ReadResolver;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Transaction filter which performs basic transactional filtering (i.e. row visibility, tombstones,
 * anti-tombstones, etc.)
 *
 * @author Scott Fines
 * Date: 6/23/14
 */
public class SimpleTxnFilter implements TxnFilter {
		private final TxnSupplier transactionStore;
		private final Txn myTxn;
		private final DataStore<Mutation,Put,Delete,Get,Scan,IHTable> dataStore;
		private final ReadResolver readResolver;

		//per row fields
		private final LongOpenHashSet visitedTxnIds = new LongOpenHashSet();
		private final LongArrayList tombstonedTxnRows = new LongArrayList(1); //usually, there are very few deletes
		private final LongArrayList antiTombstonedTxnRows = new LongArrayList(1);
		private final ByteSlice rowKey = new ByteSlice();

		@SuppressWarnings("unchecked")
		public SimpleTxnFilter(TxnSupplier transactionStore,
													 Txn myTxn,
													 ReadResolver readResolver,
													 DataStore dataStore) {
				this.transactionStore = new ActiveTxnCacheSupplier(transactionStore, SIConstants.activeTransactionCacheSize); //cache active transactions, but only on this thread
				this.myTxn = myTxn;
				this.readResolver = readResolver;
				this.dataStore = dataStore;
		}

		@Override
		public KeyValueType getType(KeyValue keyValue) throws IOException {
				return dataStore.getKeyValueType(keyValue);
		}

		@Override
		public Filter.ReturnCode filterKeyValue(KeyValue keyValue) throws IOException {
				KeyValueType type = dataStore.getKeyValueType(keyValue);
				if(type==KeyValueType.COMMIT_TIMESTAMP){
						ensureTransactionIsCached(keyValue);
						return Filter.ReturnCode.SKIP;
				}

				readResolve(keyValue);
				switch(dataStore.getKeyValueType(keyValue)){
						case TOMBSTONE:
								addToTombstoneCache(keyValue);
								return Filter.ReturnCode.SKIP;
						case ANTI_TOMBSTONE:
								addToAntiTombstoneCache(keyValue);
								return Filter.ReturnCode.SKIP;
						case USER_DATA:
								return checkVisibility(keyValue);
						default:
								//TODO -sf- do better with this?
								throw new AssertionError("Unexpected Data type: "+ dataStore.getKeyValueType(keyValue));
				}
		}

		@Override
		public void nextRow() {
				//clear row-specific fields
				visitedTxnIds.clear();
				tombstonedTxnRows.clear();
				antiTombstonedTxnRows.clear();
				rowKey.reset();
		}

		@Override public KeyValue produceAccumulatedKeyValue() { return null; }
		@Override public boolean getExcludeRow() { return false; }


		private void readResolve(KeyValue keyValue) throws IOException {
				/*
				 * We want to resolve the transaction related
				 * to this version of the data.
				 *
				 * The point of the commit timestamp column (and thus the read resolution)
				 * is that the transaction is known to be committed, and requires NO FURTHER
				 * information to ensure its visibility (e.g. that all we need to ensure visibility
				 * is the commit timestamp itself).
				 *
				 * We only enter this method if we DO NOT have a commit timestamp for this version
				 * of the data. In this case, we want to determine if we can or cannot resolve
				 * this column as committed (and thus add a commit timestamp entry). If the data
				 * was written by a dependent child transaction, the proper commit timestamp is
				 * NOT the commit timestamp of the child, it is the commit timestamp of the parent,
				 * which means that we'll need to use the effective commit timestamp and the effective
				 * state to determine whether or not to read-resolve the entry.
				 *
				 * This means that we will NOT writes from dependent child transactions until their
				 * parent transaction has been committed.
				 */
				long ts = keyValue.getTimestamp();
				if(visitedTxnIds.contains(ts)){
						/*
						 * We've already visited this version of the row data, so there's no
						 * point in read-resolving this entry. This saves us from a
						 * potentially expensive transactionStore read.
						 */
					return;
				}
				visitedTxnIds.add(ts); //prevent future columns from performing read-resolution on the same row
				Txn t = transactionStore.getTransaction(ts);
				//get the row data. This will allow efficient movement of the row key without copying byte[]s
				rowKey.set(keyValue.getBuffer(),keyValue.getRowOffset(),keyValue.getRowLength());

				if(t.getEffectiveState()==Txn.State.COMMITTED){
						/*
						 * This entry has definitely FOR SURE been committed, and all we need
						 * is the effective commit timestamp for the transaction--thus,
						 * we can perform a read-resolution on it.
						 */
						readResolver.resolveCommitted(rowKey, ts, t.getEffectiveCommitTimestamp());
				}else if(t.getEffectiveState()==Txn.State.ROLLEDBACK){
						//this entry has been rolled back, and can be removed from physical storage
						readResolver.resolveRolledback(rowKey,ts);
				}
		}

		private Filter.ReturnCode checkVisibility(KeyValue keyValue) throws IOException {
				/*
				 * First, we check to see if we are covered by a tombstone--that is,
				 * if keyValue has a timestamp <= the timestamp of a tombstone AND our isolationLevel
				 * allows us to see the tombstone, then this row no longer exists for us, and
				 * we should just skip the column.
				 *
				 * Otherwise, we just look at the transaction of the entry's visibility to us--if
				 * it matches, then we can see it.
				 */
				long timestamp = keyValue.getTimestamp();
				long[] tombstones = tombstonedTxnRows.buffer;
				int tombstoneSize = tombstonedTxnRows.size();
				for(int i=0;i<tombstoneSize;i++){
						long tombstone = tombstones[i];
						if(isVisible(tombstone)&& timestamp<=tombstone)
								return Filter.ReturnCode.NEXT_COL;
				}

				long[] antiTombstones = antiTombstonedTxnRows.buffer;
				int antiTombstoneSize = antiTombstonedTxnRows.size();
				for(int i=0;i<antiTombstoneSize;i++){
						long antiTombstone = antiTombstones[i];
						if(isVisible(antiTombstone)&& timestamp<antiTombstone)
								return Filter.ReturnCode.NEXT_COL;
				}

				//we don't have any tombstone problems, so just check our own visibility
				if(!isVisible(timestamp)) return Filter.ReturnCode.SKIP;
				return Filter.ReturnCode.INCLUDE;
		}

		private boolean isVisible(long txnId) throws IOException {
				Txn otherTxn = transactionStore.getTransaction(txnId);
				return myTxn.canSee(otherTxn);
		}

		private void addToAntiTombstoneCache(KeyValue kv) throws IOException {
				long txnId = kv.getTimestamp();
				Txn otherTxn = transactionStore.getTransaction(txnId);
				if(myTxn.canSee(otherTxn)){
						/*
						 * We can see this anti-tombstone, hooray!
						 */
						if(!tombstonedTxnRows.contains(txnId))
								antiTombstonedTxnRows.add(txnId);
				}
		}

		private void addToTombstoneCache(KeyValue kv) throws IOException {
				long txnId = kv.getTimestamp();
				/*
				 * Only add a tombstone to our list if it's actually visible,
				 * otherwise there's no point, since we can't see it anyway.
				 */
				Txn transaction = transactionStore.getTransaction(txnId);
				if(myTxn.canSee(transaction)){
						if(!antiTombstonedTxnRows.contains(txnId))
								tombstonedTxnRows.add(txnId);
				}
		}

		private void ensureTransactionIsCached(KeyValue keyValue) {
				long txnId = keyValue.getTimestamp();
				if(!transactionStore.transactionCached(txnId)){
						/*
						 * We do not have a cache entry for this transaction, so we want
						 * to add it in. We have two possible scenarios:
						 *
						 * 1. The transaction is marked as "failed"--i.e. it's been rolled back
						 * 2. The transaction has a commit timestamp.
						 *
						 * In case #1, we only care about the txnId, which we already have,
						 * and in case #2, we only care about the commit timestamp (none of the read
						 * isolation levels require information about the begin timestamp).
						 *
						 * This version of SI will physically remove entries associated
						 * with rolled-back values, so case #1 isn't likely to happen--however,
						 * older installations of SpliceMachine used the commit timestamp to indicate
						 * a failure, so we have to check for it.
						 */
						if(dataStore.isSIFail(keyValue)){
								transactionStore.cache(new RolledBackTxn(txnId));
						}else{
								long commitTs = Bytes.toLong(keyValue.getBuffer(),keyValue.getValueOffset(),keyValue.getValueLength());
								transactionStore.cache(new CommittedTxn(txnId,commitTs)); //since we don't care about the begin timestamp, just use the TxnId
								visitedTxnIds.add(txnId);
						}
				}
		}
}
