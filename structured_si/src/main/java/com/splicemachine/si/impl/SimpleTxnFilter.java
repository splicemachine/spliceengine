package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.api.ReadResolver;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.data.api.IHTable;
import com.splicemachine.si.impl.store.ActiveTxnCacheSupplier;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import java.io.IOException;

/**
 * Transaction filter which performs basic transactional filtering (i.e. row visibility, tombstones,
 * anti-tombstones, etc.)
 *
 * @author Scott Fines
 * Date: 6/23/14
 */
public class SimpleTxnFilter<RowLock,Data> implements TxnFilter<Data> {
		private final TxnSupplier transactionStore;
		private final TxnView myTxn;
		private final DataStore<RowLock,Data,Mutation,Put,Delete,Get,Scan,IHTable> dataStore;
		private final ReadResolver readResolver;
		//per row fields
		private final LongOpenHashSet visitedTxnIds = new LongOpenHashSet();
		private final LongArrayList tombstonedTxnRows = new LongArrayList(1); //usually, there are very few deletes
		private final LongArrayList antiTombstonedTxnRows = new LongArrayList(1);
		private final ByteSlice rowKey = new ByteSlice();

    /*
     * The most common case for databases is insert-only--that is, that there
     * are few updates and deletes relative to the number of inserts. As a result,
     * we can assume that *most* rows will only have a single version of data *most*
     * of the time. This enables us to bypass transaction caching whenever we have
     * a transaction--that is, the first time we look up a transaction, we stash it
     * in this variable. Then, the next time we want the transaction information, we check
     * this variable first. That way, we avoid potentially expensive locking through the
     * transaction supplier.
     */
    private TxnView currentTxn;

		@SuppressWarnings("unchecked")
		public SimpleTxnFilter(TxnSupplier transactionStore,
													 TxnView myTxn,
													 ReadResolver readResolver,
													 DataStore dataStore) {
				this.transactionStore = new ActiveTxnCacheSupplier(transactionStore, SIConstants.activeTransactionCacheSize); //cache active transactions, but only on this thread
				this.myTxn = myTxn;
				this.readResolver = readResolver;
				this.dataStore = dataStore;
		}

		@Override
		public KeyValueType getType(Data element) throws IOException {
				return dataStore.getKeyValueType(element);
		}

		@Override
		public Filter.ReturnCode filterKeyValue(Data element) throws IOException {
				KeyValueType type = dataStore.getKeyValueType(element);
				if(type==KeyValueType.COMMIT_TIMESTAMP){
						ensureTransactionIsCached(element);
						return Filter.ReturnCode.SKIP;
				}

				readResolve(element);
				switch(dataStore.getKeyValueType(element)){
						case TOMBSTONE:
								addToTombstoneCache(element);
								return Filter.ReturnCode.SKIP;
						case ANTI_TOMBSTONE:
								addToAntiTombstoneCache(element);
								return Filter.ReturnCode.SKIP;
						case USER_DATA:
								return checkVisibility(element);
						default:
								//TODO -sf- do better with this?
								throw new AssertionError("Unexpected Data type: "+ dataStore.getKeyValueType(element));
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

		@Override public Data produceAccumulatedKeyValue() { return null; }
		@Override public boolean getExcludeRow() { return false; }


		private void readResolve(Data element) throws IOException {
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
				long ts = dataStore.dataLib.getTimestamp(element);
        if(!visitedTxnIds.add(ts)){
						/*
						 * We've already visited this version of the row data, so there's no
						 * point in read-resolving this entry. This saves us from a
						 * potentially expensive transactionStore read.
						 */
					return;
				}

				TxnView t = fetchTransaction(ts);
				assert t!=null :"Could not find a transaction for id "+ ts;

        //submit it to the resolver to resolve asynchronously
        if(t.getEffectiveState().isFinal()){
            doResolve(element, ts);
        }
    }

    protected void doResolve(Data data, long ts) {
        //get the row data. This will allow efficient movement of the row key without copying byte[]s
    	dataStore.dataLib.setRowInSlice(data, rowKey);
        readResolver.resolve(rowKey,ts);
    }

    private Filter.ReturnCode checkVisibility(Data data) throws IOException {
				/*
				 * First, we check to see if we are covered by a tombstone--that is,
				 * if keyValue has a timestamp <= the timestamp of a tombstone AND our isolationLevel
				 * allows us to see the tombstone, then this row no longer exists for us, and
				 * we should just skip the column.
				 *
				 * Otherwise, we just look at the transaction of the entry's visibility to us--if
				 * it matches, then we can see it.
				 */
				long timestamp = dataStore.dataLib.getTimestamp(data);
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
        TxnView toCompare = fetchTransaction(txnId);
				return myTxn.canSee(toCompare);
		}

    private TxnView fetchTransaction(long txnId) throws IOException {
        TxnView toCompare = currentTxn;
        if(currentTxn==null || currentTxn.getTxnId()!=txnId){
            toCompare = transactionStore.getTransaction(txnId);
            currentTxn = toCompare;
        }
        return toCompare;
    }

    private void addToAntiTombstoneCache(Data data) throws IOException {
				long txnId = this.dataStore.dataLib.getTimestamp(data);
        if(isVisible(txnId)){
						/*
						 * We can see this anti-tombstone, hooray!
						 */
						if(!tombstonedTxnRows.contains(txnId))
								antiTombstonedTxnRows.add(txnId);
				}
		}

		private void addToTombstoneCache(Data data) throws IOException {
			long txnId = this.dataStore.dataLib.getTimestamp(data);
				/*
				 * Only add a tombstone to our list if it's actually visible,
				 * otherwise there's no point, since we can't see it anyway.
				 */
        if(isVisible(txnId)){
						if(!antiTombstonedTxnRows.contains(txnId))
								tombstonedTxnRows.add(txnId);
				}
		}

		private void ensureTransactionIsCached(Data data) {
			long txnId = this.dataStore.dataLib.getTimestamp(data);
        visitedTxnIds.add(txnId);
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
            TxnView toCache;
						if(dataStore.isSIFail(data)){
								//use the current read-resolver to remove the entry
                doResolve(data, dataStore.dataLib.getTimestamp(data));
                toCache = new RolledBackTxn(txnId);
						}else{
								long commitTs = dataStore.dataLib.getValueToLong(data);
                toCache = new CommittedTxn(txnId, commitTs);//since we don't care about the begin timestamp, just use the TxnId
						}
            transactionStore.cache(toCache);
            currentTxn = toCache;
				}
		}

		@Override
		public DataStore getDataStore() {
			return dataStore;
		}
}
