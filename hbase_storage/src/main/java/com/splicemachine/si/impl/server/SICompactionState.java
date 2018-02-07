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

package com.splicemachine.si.impl.server;

import com.google.common.util.concurrent.Futures;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.txn.TransactionMissing;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.store.ActiveTxnCacheSupplier;
import com.splicemachine.si.impl.txn.CommittedTxn;
import com.splicemachine.si.impl.txn.RolledBackTxn;
import com.splicemachine.storage.CellType;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

/**
 * Captures the SI logic to perform when a data table is compacted (without explicit HBase dependencies). Provides the
 * guts for SICompactionScanner.
 * <p/>
 * It is handed key-values and can change them.
 */
public class SICompactionState {
    private static final Logger LOG = Logger.getLogger(SICompactionState.class);
    private final TxnSupplier transactionStore;
    private final CompactionContext context;
    private final ExecutorService executorService;
    private ConcurrentHashMap<Long, Future<TxnView>> futuresCache;
    private SortedSet<Cell> dataToReturn;

    public SICompactionState(TxnSupplier transactionStore, int activeTransactionCacheSize, CompactionContext context, ExecutorService executorService) {
        this.transactionStore = new ActiveTxnCacheSupplier(transactionStore,activeTransactionCacheSize);
        this.dataToReturn  =new TreeSet<>(KeyValue.COMPARATOR);
        this.context = context;
        this.futuresCache = new ConcurrentHashMap<>(1<<19, 0.75f, 64);
        this.executorService = executorService;
    }

    /**
     * Given a list of key-values, populate the results list with possibly mutated values.
     *
     * @param rawList - the input of key values to process
     * @param results - the output key values
     */
    public void mutate(List<Cell> rawList, List<TxnView> txns, List<Cell> results, boolean purgeDeletedRows) throws IOException {
        dataToReturn.clear();
        long maxTombstone = 0;
        Iterator<TxnView> it = txns.iterator();
        for (Cell aRawList : rawList) {
            TxnView txn = it.next();
            long t = mutate(aRawList, txn);
            if (t > maxTombstone) {
                maxTombstone = t;
            }
        }
        if (purgeDeletedRows && maxTombstone > 0) {
            removeTombStone(maxTombstone);
        }
        results.addAll(dataToReturn);
    }

    private void removeTombStone(long maxTombstone) {
        SortedSet<Cell> cp = (SortedSet<Cell>)((TreeSet<Cell>)dataToReturn).clone();
        for (Cell element : cp) {
            long timestamp = element.getTimestamp();
            if (timestamp <= maxTombstone) {
                dataToReturn.remove(element);
            }
        }
    }
    /**
     * Apply SI mutation logic to an individual key-value. Return the "new" key-value.
     */
    private long mutate(Cell element, TxnView txn) throws IOException {
        final CellType cellType= getKeyValueType(element);
        long timestamp = element.getTimestamp();
        switch (cellType) {
            case COMMIT_TIMESTAMP:
                dataToReturn.add(element);
                return 0;
            default:
                if(mutateCommitTimestamp(element,txn))
                    dataToReturn.add(element);
                if (cellType == CellType.TOMBSTONE) {
                    return timestamp;
                }
                else {
                    return 0;
                }
        }
    }

    private void ensureTransactionCached(long timestamp,Cell element) {
        if(!transactionStore.transactionCached(timestamp)){
            if(isFailedCommitTimestamp(element)){
                transactionStore.cache(new RolledBackTxn(timestamp));
            }else if (element.getValueLength()>0){ //shouldn't happen, but you never know
                long commitTs = Bytes.toLong(element.getValueArray(),element.getValueOffset(),element.getValueLength());

                if (LOG.isDebugEnabled())
                    LOG.debug("Caching " + timestamp + " with commitTs " + commitTs);
                transactionStore.cache(new CommittedTxn(timestamp,commitTs));
            }
        }
    }

    /**
     * Replace unknown commit timestamps with actual commit times.
     */
    private boolean mutateCommitTimestamp(Cell element, TxnView txn) throws IOException {
        if (txn == null) {
            // we don't have transactional information, just return the data as is
            return true;
        } else if (txn.getState() == Txn.State.ROLLEDBACK) {
            // rolled back data, remove it from the compacted data
            return false;
        } else if (txn.getState() == Txn.State.COMMITTED && txn.getParentTxnView() == Txn.ROOT_TRANSACTION) {
            /*
             * This element has been committed all the way to the user level, so a
             * commit timestamp can be placed on it.
             */
            long globalCommitTimestamp = txn.getEffectiveCommitTimestamp();
            dataToReturn.add(newTransactionTimeStampKeyValue(element, Bytes.toBytes(globalCommitTimestamp)));
        }
        // Committed or active, return the original data too
        return true;
    }

    public Cell newTransactionTimeStampKeyValue(Cell element, byte[] value) {
        return new KeyValue(element.getRowArray(),
                element.getRowOffset(),
                element.getRowLength(),
                SIConstants.DEFAULT_FAMILY_BYTES,0,1,
                SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,0,1,
                element.getTimestamp(),KeyValue.Type.Put,
                value,0,value==null?0:value.length);
    }


    public boolean isFailedCommitTimestamp(Cell element) {
        return element.getValueLength()==1 && element.getValueArray()[element.getValueOffset()]==SIConstants.SNAPSHOT_ISOLATION_FAILED_TIMESTAMP[0];
    }

    public CellType getKeyValueType(Cell keyValue) {
        if (CellUtils.singleMatchingQualifier(keyValue,SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES)) {
            return CellType.COMMIT_TIMESTAMP;
        } else if (CellUtils.singleMatchingQualifier(keyValue, SIConstants.PACKED_COLUMN_BYTES)) {
            return CellType.USER_DATA;
        } else if (CellUtils.singleMatchingQualifier(keyValue,SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES)) {
            if (CellUtils.matchingValue(keyValue, SIConstants.EMPTY_BYTE_ARRAY)) {
                return CellType.TOMBSTONE;
            } else if (CellUtils.matchingValue(keyValue,SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES)) {
                return CellType.ANTI_TOMBSTONE;
            }
        } else if (CellUtils.singleMatchingQualifier(keyValue, SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES)) {
            return CellType.FOREIGN_KEY_COUNTER;
        }
        return CellType.OTHER;
    }


    public List<Future<TxnView>> resolve(List<Cell> list) throws IOException {
        context.rowRead();
        List<Future<TxnView>> result = new ArrayList<>(list.size());
        for (Cell element : list) {
            final CellType cellType= getKeyValueType(element);
            long timestamp = element.getTimestamp();
            switch (cellType) {
                case COMMIT_TIMESTAMP:
                    /*
                     * Older versions of SI code would put an "SI Fail" element in the commit timestamp
                     * field when a row has been rolled back. While newer versions will just outright delete the entry,
                     * we still need to deal with entries which are in the old form. As time goes on, this should
                     * be less and less frequent, but you still have to check
                     */
                    ensureTransactionCached(timestamp,element);
                    result.add(null); // no transaction needed for this entry
                    context.readCommit();
                    break;
                case TOMBSTONE:
                case ANTI_TOMBSTONE:
                case USER_DATA:
                default:
                    context.readData();
                    TxnView tentative = transactionStore.getTransactionFromCache(timestamp);
                    if (tentative != null) {
                        if (LOG.isDebugEnabled())
                            LOG.debug("Cached " + tentative);
                        result.add(Futures.immediateFuture(tentative));
                        context.recordResolutionCached();
                    } else {
                        Future<TxnView> future;
                        try {
                            future = futuresCache.computeIfAbsent(timestamp, txnId -> {
                                context.recordRPC();
                                return executorService.submit(() -> {
                                    if (LOG.isDebugEnabled())
                                        LOG.debug("Resolving " + txnId);
                                    TxnView txn;
                                    try {
                                        txn = transactionStore.getTransaction(txnId);

                                        if (LOG.isTraceEnabled())
                                            LOG.trace("Txn " + txn);
                                        while (txn.getState() == Txn.State.COMMITTED && txn.getParentTxnView() != Txn.ROOT_TRANSACTION) {
                                            txn = txn.getParentTxnView();

                                            if (LOG.isTraceEnabled())
                                                LOG.trace("Parent " + txn);
                                        }
                                    } catch (TransactionMissing ex) {
                                        txn = null;
                                    }
                                    if (txn == null) {
                                        LOG.warn("We couldn't resolve transaction " + timestamp +". This is only acceptable during a Restore operation");
                                        return null;
                                    }
                                    if (LOG.isDebugEnabled())
                                        LOG.debug("Returning, parent " + txn.getParentTxnView());
                                    return txn;
                                });
                            });
                            context.recordResolutionScheduled();
                        } catch (RejectedExecutionException ex) {
                            context.recordResolutionRejected();
                            future = Futures.immediateFuture(null);
                        }
                        result.add(future);
                    }
            }
        }
        return result;
    }

    /** Remove entry from futures cache after it is already available in the transactional cache*/
    public void remove(long txnId) {
        futuresCache.remove(txnId);
    }
}
