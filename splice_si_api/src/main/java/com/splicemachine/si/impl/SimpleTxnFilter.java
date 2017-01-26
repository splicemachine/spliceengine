/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.splicemachine.si.api.filter.RowAccumulator;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.store.ActiveTxnCacheSupplier;
import com.splicemachine.si.impl.txn.CommittedTxn;
import com.splicemachine.storage.CellType;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataFilter;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;

/**
 * Transaction filter which performs basic transactional filtering (i.e. row visibility, tombstones,
 * anti-tombstones, etc.)
 *
 * @author Scott Fines
 *         Date: 6/23/14
 */
public class SimpleTxnFilter implements TxnFilter{
    private final TxnSupplier transactionStore;
    private final TxnView myTxn;
    private final ReadResolver readResolver;
    //per row fields
    private final LongOpenHashSet visitedTxnIds=new LongOpenHashSet();
    private final LongArrayList tombstonedTxnRows=new LongArrayList(1); //usually, there are very few deletes
    private final LongArrayList antiTombstonedTxnRows=new LongArrayList(1);
    private final ByteSlice rowKey=new ByteSlice();
    private final String tableName;

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
    public SimpleTxnFilter(String tableName,
                           TxnView myTxn,
                           ReadResolver readResolver,
                           TxnSupplier baseSupplier){
        assert readResolver!=null;
        this.transactionStore = new ActiveTxnCacheSupplier(baseSupplier,1024); //TODO -sf- configure
        this.tableName=tableName;
        this.myTxn=myTxn;
        this.readResolver=readResolver;
    }

    @Override
    public boolean filterRow(){
        return getExcludeRow();
    }

    @Override
    public void reset(){
        nextRow();
    }

    @Override
    public DataFilter.ReturnCode filterCell(DataCell keyValue) throws IOException{
        CellType type=keyValue.dataType();
        if(type==CellType.COMMIT_TIMESTAMP){
            ensureTransactionIsCached(keyValue);
            return DataFilter.ReturnCode.SKIP;
        }
        if(type==CellType.FOREIGN_KEY_COUNTER){
            /* Transactional reads always ignore this column, no exceptions. */
            return DataFilter.ReturnCode.SKIP;
        }

        readResolve(keyValue);
        switch(type){
            case TOMBSTONE:
                addToTombstoneCache(keyValue);
                return DataFilter.ReturnCode.SKIP;
            case ANTI_TOMBSTONE:
                addToAntiTombstoneCache(keyValue);
                return DataFilter.ReturnCode.SKIP;
            case USER_DATA:
                return checkVisibility(keyValue);
            default:
                //TODO -sf- do better with this?
                throw new AssertionError("Unexpected Data type: "+type);
        }
    }

    @Override
    public DataCell produceAccumulatedResult(){
        return null;
    }

    @Override
    public void nextRow(){
        //clear row-specific fields
        visitedTxnIds.clear();
        tombstonedTxnRows.clear();
        antiTombstonedTxnRows.clear();
        rowKey.reset();
    }

    @Override
    public boolean getExcludeRow(){
        return false;
    }


    private void readResolve(DataCell element) throws IOException{
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
        long ts=element.version();//dataStore.getOpFactory().getTimestamp(element);
        if(!visitedTxnIds.add(ts)){
			/*
			 * We've already visited this version of the row data, so there's no
			 * point in read-resolving this entry. This saves us from a
			 * potentially expensive transactionStore read.
			 */
            return;
        }

        TxnView t=fetchTransaction(ts);
        assert t!=null:"Could not find a transaction for id "+ts;

        //submit it to the resolver to resolve asynchronously
        if(t.getEffectiveState().isFinal()){
            doResolve(element,ts);
        }
    }

    protected void doResolve(DataCell data,long ts){
        //get the row data. This will allow efficient movement of the row key without copying byte[]s
        rowKey.set(data.keyArray(),data.keyOffset(),data.keyLength());
        readResolver.resolve(rowKey,ts);
    }

    private DataFilter.ReturnCode checkVisibility(DataCell data) throws IOException{
		/*
		 * First, we check to see if we are covered by a tombstone--that is,
		 * if keyValue has a timestamp <= the timestamp of a tombstone AND our isolationLevel
		 * allows us to see the tombstone, then this row no longer exists for us, and
		 * we should just skip the column.
		 *
		 * Otherwise, we just look at the transaction of the entry's visibility to us--if
		 * it matches, then we can see it.
		 */
        long timestamp=data.version();//dataStore.getOpFactory().getTimestamp(data);
        long[] tombstones=tombstonedTxnRows.buffer;
        int tombstoneSize=tombstonedTxnRows.size();
        for(int i=0;i<tombstoneSize;i++){
            long tombstone=tombstones[i];
            if(isVisible(tombstone) && timestamp<=tombstone)
                return DataFilter.ReturnCode.NEXT_ROW;
        }

        long[] antiTombstones=antiTombstonedTxnRows.buffer;
        int antiTombstoneSize=antiTombstonedTxnRows.size();
        for(int i=0;i<antiTombstoneSize;i++){
            long antiTombstone=antiTombstones[i];
            if(isVisible(antiTombstone) && timestamp<antiTombstone)
                return DataFilter.ReturnCode.NEXT_ROW;
        }

        //we don't have any tombstone problems, so just check our own visibility
        if(!isVisible(timestamp)) return DataFilter.ReturnCode.SKIP;
        return DataFilter.ReturnCode.INCLUDE;
    }

    private boolean isVisible(long txnId) throws IOException{
        TxnView toCompare=fetchTransaction(txnId);
        return myTxn.canSee(toCompare);
    }

    private TxnView fetchTransaction(long txnId) throws IOException{
        TxnView toCompare=currentTxn;
        if(currentTxn==null || currentTxn.getTxnId()!=txnId){
            toCompare=transactionStore.getTransaction(txnId);
            currentTxn=toCompare;
        }
        return toCompare;
    }

    private void addToAntiTombstoneCache(DataCell data) throws IOException{
        long txnId=data.version();
        if(isVisible(txnId)){
			/*
			 * We can see this anti-tombstone, hooray!
			 */
            if(!tombstonedTxnRows.contains(txnId))
                antiTombstonedTxnRows.add(txnId);
        }
    }

    private void addToTombstoneCache(DataCell data) throws IOException{
        long txnId=data.version();//this.dataStore.getOpFactory().getTimestamp(data);
		/*
		 * Only add a tombstone to our list if it's actually visible,
		 * otherwise there's no point, since we can't see it anyway.
		 */
        if(isVisible(txnId)){
            if(!antiTombstonedTxnRows.contains(txnId))
                tombstonedTxnRows.add(txnId);
        }
    }

    private void ensureTransactionIsCached(DataCell data) throws IOException{
        long txnId=data.version();//this.dataStore.getOpFactory().getTimestamp(data);
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

            long commitTs=data.valueAsLong();//dataStore.getOpFactory().getValueToLong(data);
            TxnView toCache=new CommittedTxn(txnId,commitTs);//since we don't care about the begin timestamp, just use the TxnId
            transactionStore.cache(toCache);
            currentTxn=toCache;
        }
    }


    @Override
    public RowAccumulator getAccumulator(){
        throw new RuntimeException("not implemented");
    }
}
