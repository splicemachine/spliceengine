/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.carrotsearch.hppc.LongHashSet;
import com.splicemachine.si.api.filter.RowAccumulator;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.store.ActiveTxnCacheSupplier;
import com.splicemachine.si.impl.store.IgnoreTxnSupplier;
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
    private IgnoreTxnSupplier ignoreTxnSupplier = null;
    //per row fields
    private final LongHashSet visitedTxnIds=new LongHashSet();
    private Long tombstonedTxnRow = null;
    private Long antiTombstonedTxnRow = null;
    private final ByteSlice rowKey=new ByteSlice();
    private boolean isReplica;

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
    private static int MAX_CACHED = 8;
    private TxnView[] localCache = new TxnView[MAX_CACHED];
    private int cacheIdx = 0;

    private void cacheLocally(TxnView txn) {
        localCache[cacheIdx] = txn;
        cacheIdx = (cacheIdx + 1) & (MAX_CACHED - 1);
    }

    private TxnView checkLocally(long txnId) {
        for (TxnView txn : localCache) {
            if (txn != null && txn.getTxnId() == txnId) {
                return txn;
            }
        }
        return null;
    }

    /**
     * In some cases we can safely ignore any data with a txnId greater than our
     * transaction begin timestamp, for instance during Spark reads
     */
    private final boolean ignoreNewerTransactions;

    public SimpleTxnFilter(String tableName,
                           TxnView myTxn,
                           ReadResolver readResolver,
                           TxnSupplier baseSupplier,
                           boolean ignoreNewerTransactions) {
        assert readResolver!=null;
        this.myTxn=myTxn;
        this.readResolver=readResolver;
        this.ignoreNewerTransactions = ignoreNewerTransactions;
        SIDriver driver = SIDriver.driver();
        if (driver == null) {
            // only happens during testing
            this.transactionStore = new ActiveTxnCacheSupplier(baseSupplier, 128, 2048);
        } else {
            ignoreTxnSupplier = driver.getIgnoreTxnSupplier();
            isReplica = driver.lifecycleManager().getReplicationRole().equals(SIConstants.REPLICATION_ROLE_REPLICA);
            int maxSize = driver.getConfiguration().getActiveTransactionMaxCacheSize();
            int initialSize = driver.getConfiguration().getActiveTransactionInitialCacheSize();
            this.transactionStore = new ActiveTxnCacheSupplier(baseSupplier, initialSize, maxSize);
        }
    }

    @SuppressWarnings("unchecked")
    public SimpleTxnFilter(String tableName,
                           TxnView myTxn,
                           ReadResolver readResolver,
                           TxnSupplier baseSupplier){
        this(tableName, myTxn, readResolver, baseSupplier, false);
    }

    @Override
    public boolean filterRow(){
        return getExcludeRow();
    }

    @Override
    public void reset(){
        nextRow();
    }

    public TxnSupplier getTxnSupplier() {
        return transactionStore;
    }

    @Override
    public DataFilter.ReturnCode filterCell(DataCell keyValue) throws IOException{
        CellType type=keyValue.dataType();
        switch (type) {
            case COMMIT_TIMESTAMP:
                ensureTransactionIsCached(keyValue);
                return DataFilter.ReturnCode.SKIP;
            case FOREIGN_KEY_COUNTER:
            case FIRST_WRITE_TOKEN:
            case DELETE_RIGHT_AFTER_FIRST_WRITE_TOKEN:
                /* Transactional reads always ignore these columns, no exceptions. */
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
        tombstonedTxnRow = null;
        antiTombstonedTxnRow = null;
        rowKey.reset();
    }

    @Override
    public boolean getExcludeRow(){
        return false;
    }


    private void readResolve(DataCell element) throws IOException{
        if (!readResolver.enabled())
            return;

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
        if (ignoreTxnSupplier != null && ignoreTxnSupplier.shouldIgnore(timestamp))
            return DataFilter.ReturnCode.SKIP;
        if(tombstonedTxnRow != null && timestamp<= tombstonedTxnRow)
            return DataFilter.ReturnCode.NEXT_ROW;

        if(antiTombstonedTxnRow != null && timestamp< antiTombstonedTxnRow)
            return DataFilter.ReturnCode.NEXT_ROW;

        //we don't have any tombstone problems, so just check our own visibility
        if(!isVisible(timestamp)) return DataFilter.ReturnCode.SKIP;
        return DataFilter.ReturnCode.INCLUDE;
    }

    private boolean isVisible(long txnId) throws IOException{
        if (ignoreNewerTransactions && myTxn.getBeginTimestamp() < txnId)
            return false;

        TxnView toCompare=fetchTransaction(txnId);

        if (isReplica && toCompare != null) {
            long committedTs = toCompare.getCommitTimestamp();
            if (committedTs == -1 || myTxn.getBeginTimestamp() < committedTs){
                return false;
            }
        }

        // If the database is restored from a backup, it may contain data that were written by a transaction which
        // is not present in SPLICE_TXN table, because SPLICE_TXN table is copied before the transaction begins.
        // However, the table written by the txn was copied
        return toCompare != null ? myTxn.canSee(toCompare) : false;
    }

    private TxnView fetchTransaction(long txnId) throws IOException {
        TxnView txn = checkLocally(txnId);
        if (txn == null) {
            txn = transactionStore.getTransaction(txnId);
            if (txn != null) {
                cacheLocally(txn);
            }
        }
        return txn;
    }

    private void addToAntiTombstoneCache(DataCell data) throws IOException{
        long txnId=data.version();
        /*
         * Check if we can see this anti-tombstone
         */
        boolean empty = antiTombstonedTxnRow == null;
        boolean tombstoned = tombstonedTxnRow != null && tombstonedTxnRow == txnId;
        if(empty && !tombstoned && isVisible(txnId)){
            antiTombstonedTxnRow = txnId;
        }
    }

    private void addToTombstoneCache(DataCell data) throws IOException{
        long txnId=data.version();
		/*
		 * Only add a tombstone to our list if it's actually visible,
		 * otherwise there's no point, since we can't see it anyway.
		 */
        boolean empty = tombstonedTxnRow == null;
        boolean antiTombstoned = antiTombstonedTxnRow != null && antiTombstonedTxnRow == txnId;
        if(empty && !antiTombstoned && isVisible(txnId)) {
            tombstonedTxnRow = txnId;
        }
    }

    private void ensureTransactionIsCached(DataCell data) throws IOException{
        long txnId = data.version();
        if (checkLocally(txnId) == null) {
            long commitTs = data.valueAsLong();
            cacheLocally(new CommittedTxn(txnId, commitTs));
        }
    }


    @Override
    public RowAccumulator getAccumulator(){
        throw new RuntimeException("not implemented");
    }
}
