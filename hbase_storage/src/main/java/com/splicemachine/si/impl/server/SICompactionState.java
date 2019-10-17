/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import org.spark_project.guava.util.concurrent.Futures;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TransactionMissing;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.store.ActiveTxnCacheSupplier;
import com.splicemachine.si.impl.txn.CommittedTxn;
import com.splicemachine.si.impl.txn.RolledBackTxn;
import com.splicemachine.storage.CellType;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
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
    private LinkedList<Cell> dataToReturn;
    private List<Cell> newCommits;

    public SICompactionState(TxnSupplier transactionStore, int activeTransactionCacheSize, CompactionContext context, ExecutorService executorService) {
        this.transactionStore = new ActiveTxnCacheSupplier(transactionStore,activeTransactionCacheSize,true);
        this.dataToReturn = new LinkedList<>();
        this.newCommits = new ArrayList<>();
        this.context = context;
        this.futuresCache = new ConcurrentHashMap<>(1<<19, 0.75f, 64);
        this.executorService = executorService;
    }

    public static boolean isSorted(List<Cell> cells) {
        if (cells.isEmpty() || cells.size() == 1) {
            return true;
        }

        Iterator<Cell> iter = cells.iterator();
        Cell current, previous = iter.next();
        while (iter.hasNext()) {
            current = iter.next();
            if (KeyValue.COMPARATOR.compare(previous, current) > 0) {
                return false;
            }
            previous = current;
        }
        return true;
    }

    public static boolean lastTwoElementsAreSorted(List<Cell> cells) {
        if (cells.isEmpty() || cells.size() == 1)
            return true;

        Iterator<Cell> iter = ((LinkedList<Cell>)cells).descendingIterator();
        Cell last = iter.next();
        Cell penultimate = iter.next();
        if (! (KeyValue.COMPARATOR.compare(penultimate, last) <= 0)) {
            System.out.println(penultimate);
            System.out.println(last);
            return false;
        }
        return true;
    }

    private void mergeNewCommitsInDataToReturn() {
        int expectedSize = dataToReturn.size() + newCommits.size();
        Iterator<Cell> newCommitsIter = newCommits.iterator();
        Cell newCommit = newCommitsIter.next();
        for (ListIterator<Cell> iter = dataToReturn.listIterator(); iter.hasNext(); ) {
            Cell element = iter.next();
            if (KeyValue.COMPARATOR.compare(newCommit, element) <= 0) {
                iter.previous();
                iter.add(newCommit);
                newCommit = newCommitsIter.next();
            }
        }
        for (; newCommitsIter.hasNext(); newCommit = newCommitsIter.next()) {
            dataToReturn.add(newCommit);
        }

        if (dataToReturn.size() != expectedSize) {
            throw new InternalError("Size inconsistency during the merge");
        }
    }

    /**
     * Given a list of key-values, populate the results list with possibly mutated values.
     *
     * @param rawList - the input of key values to process
     * @param results - the output key values
     */
    public void mutate(List<Cell> rawList, List<TxnView> txns, List<Cell> results, boolean purgeDeletedRows) throws IOException {
        if (!isSorted(rawList)) {
            throw new InternalError("Argument rawList is not sorted");
        }
        dataToReturn.clear();
        newCommits.clear();
        long maxTombstone = 0;
        Iterator<TxnView> it = txns.iterator();
        for (Cell aRawList : rawList) {
            TxnView txn = it.next();
            long t = mutate(aRawList, txn);
            if (t > maxTombstone) {
                maxTombstone = t;
            }
        }
        mergeNewCommitsInDataToReturn();

        if (purgeDeletedRows && maxTombstone > 0) {
            removeTombStone(maxTombstone);
        }
        results.addAll(dataToReturn);
        if (!isSorted(results)) {
            throw new InternalError("results are not sorted");
        }
    }

    private void removeTombStone(long maxTombstone) {
        for (Iterator<Cell> iter = dataToReturn.iterator(); iter.hasNext();) {
            Cell element = iter.next();
            long timestamp = element.getTimestamp();
            if (timestamp <= maxTombstone) {
                dataToReturn.remove(element);
            }
        }
    }
    /**
     * Apply SI mutation logic to an individual key-value. Add the "new" key-value to dataToReturn.
     * @return timestamp if cell is tombstone, else 0
     */
    private long mutate(Cell element, TxnView txn) throws IOException {
        final CellType cellType= CellUtils.getKeyValueType(element);
        long timestamp = element.getTimestamp();
        if (cellType == CellType.COMMIT_TIMESTAMP) {
            dataToReturn.add(element);
            return 0;
        }
        if (mutateCommitTimestamp(element,txn)) {
            dataToReturn.add(element);
        }
        if (cellType == CellType.TOMBSTONE) {
            return timestamp;
        }
        return 0;
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
        } else if (committed(txn)) {
            /*
             * This element has been committed all the way to the user level, so a
             * commit timestamp can be placed on it.
             */
            long globalCommitTimestamp = txn.getEffectiveCommitTimestamp();
            newCommits.add(newTransactionTimeStampKeyValue(element, Bytes.toBytes(globalCommitTimestamp)));
        }
        // Committed or active, return the original data too
        return true;
    }

    private boolean committed(TxnView txn) {
        while (txn.getState() == Txn.State.COMMITTED && txn.getParentTxnView() != Txn.ROOT_TRANSACTION) {
            txn = txn.getParentTxnView();
        }
        return txn.getState() == Txn.State.COMMITTED && txn.getParentTxnView() == Txn.ROOT_TRANSACTION;
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


    public List<Future<TxnView>> resolve(List<Cell> list) throws IOException {
        if (context != null)
            context.rowRead();
        List<Future<TxnView>> result = new ArrayList<>(list.size());
        for (Cell element : list) {
            final CellType cellType= CellUtils.getKeyValueType(element);
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
                    if (context != null)
                        context.readCommit();
                    break;
                case TOMBSTONE:
                case ANTI_TOMBSTONE:
                case USER_DATA:
                default:
                    if (context != null)
                        context.readData();
                    TxnView tentative = transactionStore.getTransactionFromCache(timestamp);
                    if (tentative != null) {
                        if (LOG.isDebugEnabled())
                            LOG.debug("Cached " + tentative);
                        result.add(Futures.immediateFuture(tentative));
                        if (context != null)
                            context.recordResolutionCached();
                    } else {
                        Future<TxnView> future;
                        try {
                            future = futuresCache.computeIfAbsent(timestamp, txnId -> {
                                if (context != null)
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
                            if (context != null)
                                context.recordResolutionScheduled();
                        } catch (RejectedExecutionException ex) {
                            if (context != null)
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
