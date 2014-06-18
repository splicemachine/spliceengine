package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnAccess;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 6/23/14
 */
public class TxnFilterState implements IFilterState{
		private final TxnAccess transactionStore;
		private final Txn myTxn;
		private final RollForwardQueue rollForwardQueue;
		private final DataStore<Mutation,Put,Delete,Get,Scan,IHTable> dataStore;

		//per row fields
		private final LongOpenHashSet visitedCommitTimestampCols = new LongOpenHashSet();
		private final LongArrayList tombstonedTxnRows = new LongArrayList(1); //usually, there are very few deletes
		private final LongArrayList antiTombstonedTxnRows = new LongArrayList(1);
		private final ByteSlice row = new ByteSlice();

		public TxnFilterState(TxnAccess transactionStore,
													Txn myTxn,
													RollForwardQueue rollForwardQueue,
													DataStore dataStore) {
				this.transactionStore = transactionStore;
				this.myTxn = myTxn;
				this.rollForwardQueue = rollForwardQueue;
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

				addToRollForwardIfNeeded(keyValue);
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
				//roll over any entries which need to be rolled forward
				int commitSize = visitedCommitTimestampCols.size();
				if(commitSize >0){
						long[] keys = visitedCommitTimestampCols.keys;
						boolean[] present = visitedCommitTimestampCols.allocated;
						for(int i=0;i<commitSize;i++){
								if(!present[i])continue;
								long txnId = keys[i];
								dataStore.recordRollForward(rollForwardQueue,txnId,row.getByteCopy(),false);
						}
				}

				//clear row-specific fields
				visitedCommitTimestampCols.clear();
				tombstonedTxnRows.clear();
				antiTombstonedTxnRows.clear();
				row.reset();
		}

		@Override public KeyValue produceAccumulatedKeyValue() { return null; }
		@Override public boolean getExcludeRow() { return false; }


		private void addToRollForwardIfNeeded(KeyValue keyValue) throws IOException {
				long toRollForward = keyValue.getTimestamp();
				Txn t = transactionStore.getTransaction(toRollForward);
				if(t.getState()!= Txn.State.ACTIVE){
						//we can roll this row forward
						visitedCommitTimestampCols.add(toRollForward);
						if(row.length()<=0){
								row.set(keyValue.getBuffer(),keyValue.getRowOffset(),keyValue.getRowLength());
						}
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
						if(!antiTombstonedTxnRows.contains(txnId))
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
						if(!tombstonedTxnRows.contains(txnId))
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
						 */
						if(dataStore.isSIFail(keyValue)){
								transactionStore.cache(new RolledBackTxn(txnId));
						}else{
								long commitTs = Bytes.toLong(keyValue.getBuffer(),keyValue.getValueOffset(),keyValue.getValueLength());
								transactionStore.cache(new CommittedTxn(txnId,commitTs)); //since we don't care about the begin timestamp, just use the TxnId
						}
				}
		}
}
