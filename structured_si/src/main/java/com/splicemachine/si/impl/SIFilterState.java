package com.splicemachine.si.impl;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.api.FilterState;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hbase.filter.Filter.ReturnCode.INCLUDE;
import static org.apache.hadoop.hbase.filter.Filter.ReturnCode.NEXT_COL;
import static org.apache.hadoop.hbase.filter.Filter.ReturnCode.SKIP;

/**
 * Contains the logic for performing an HBase-style filter using "snapshot isolation" logic. This means it filters out
 * data that should not be seen by the transaction that is performing the read operation (either a "get" or a "scan").
 */
public class SIFilterState implements FilterState {
    static final Logger LOG = Logger.getLogger(SIFilterState.class);

    /**
     * The transactions that have been loaded as part of running this filter.
     */
    final Cache<Long, Transaction> transactionCache;

    private final ImmutableTransaction myTransaction;
    private final SDataLib dataLib;
    private final DataStore dataStore;
    private final TransactionStore transactionStore;
    private final RollForwardQueue rollForwardQueue;
    private final boolean siOnly;

    private final FilterRowState rowState;
    private DecodedKeyValue keyValue;

    public SIFilterState(SDataLib dataLib, DataStore dataStore, TransactionStore transactionStore,
                         RollForwardQueue rollForwardQueue, boolean siOnly, ImmutableTransaction myTransaction) {
        this.dataLib = dataLib;
        this.dataStore = dataStore;
        this.transactionStore = transactionStore;
        this.rollForwardQueue = rollForwardQueue;
        this.siOnly = siOnly;
        this.myTransaction = myTransaction;

        transactionCache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(10, TimeUnit.MINUTES).build();

        // initialize internal state
        this.rowState = new FilterRowState(dataLib);
    }

    /**
     * The public entry point. This returns an HBase filter code and is expected to serve as the heart of an HBase filter
     * implementation.
     * The order of the column families is important. It is expected that the SI family will be processed first.
     */
    public Filter.ReturnCode filterKeyValue(Object dataKeyValue) throws IOException {
        keyValue = new DecodedKeyValue(dataLib, dataKeyValue);
        rowState.updateCurrentRow(keyValue.row);
        return filterByColumnType();
    }

    /**
     * Look at the column family and qualifier to determine how to "dispatch" the current keyValue.
     */
    private Filter.ReturnCode filterByColumnType() throws IOException {
        final KeyValueType type = dataStore.getKeyValueType(keyValue.family, keyValue.qualifier);
        if (type.equals(KeyValueType.COMMIT_TIMESTAMP)) {
            if (siOnly) {
                processCommitTimestamp();
                return processUserData();
            } else {
                return processCommitTimestamp();
            }
        } else if (type.equals(KeyValueType.TOMBSTONE)) {
            return processTombstone();
        } else if (type.equals(KeyValueType.USER_DATA)) {
            return processUserData();
        } else {
            return processUnknownFamilyData();
        }
    }

    /**
     * Handles the "commit timestamp" values that SI writes to each data row. Processing these values causes the
     * relevant transaction data to be loaded up into the local cache.
     */
    private Filter.ReturnCode processCommitTimestamp() throws IOException {
        Transaction transaction = null;

        if (dataStore.isSiNull(keyValue.value)) {
            transaction = handleUnknownTransactionStatus();
        } else if (dataStore.isSiFail(keyValue.value)) {
            transaction = new Transaction(keyValue.timestamp, 0, null, null, null, false, null, null, TransactionStatus.ERROR, null);
        } else {
            // TODO: we should avoid loading the full transaction here once roll-forward is revisited
            transaction = getTransactionFromFilterCache();
        }
        rowState.transactionCache.put(transaction.beginTimestamp, transaction);
        return SKIP;
    }

    private Transaction getTransactionFromFilterCache() throws IOException {
        Transaction transaction = transactionCache.getIfPresent(keyValue.timestamp);
        if (transaction == null) {
            final ImmutableTransaction immutableTransaction = transactionStore.getImmutableTransaction(keyValue.timestamp);
            if (!myTransaction.getEffectiveReadCommitted() && !myTransaction.getEffectiveReadUncommitted()
                    && (immutableTransaction.getRootBeginTimestamp() != myTransaction.getRootBeginTimestamp())) {
                transaction = transactionStore.getTransactionAsOf(keyValue.timestamp, myTransaction.getTransactionId());
            } else {
                transaction = transactionStore.getTransaction(keyValue.timestamp);
            }
            transactionCache.put(transaction.beginTimestamp, transaction);
        }
        return transaction;
    }

    /**
     * Handles the case where the commit timestamp cell contains a begin timestamp, but doesn't have an
     * associated commit timestamp in the cell. This means the transaction table needs to be consulted to find out
     * the current status of the transaction.
     */
    private Transaction handleUnknownTransactionStatus() throws IOException {
        final Transaction cachedTransaction = rowState.transactionCache.get(keyValue.timestamp);
        if (cachedTransaction == null) {
            final Transaction dataTransaction = getTransactionFromFilterCache();
            if (dataTransaction.isCommitted() || dataTransaction.isFailed()) {
                rollForward(dataTransaction);
            }
            return dataTransaction;
        } else {
            return cachedTransaction;
        }
    }

    /**
     * Update the data row to remember the commit timestamp of the transaction. This avoids the need to look the
     * transaction up in the transaction table again the next time this row is read.
     */
    private void rollForward(Transaction transaction) throws IOException {
        if (!transaction.isNestedDependent()) {
            // TODO: revisit this in light of nested independent transactions
            dataStore.recordRollForward(rollForwardQueue, transaction, keyValue.row);
        }
    }

    /**
     * Under SI, deleting a row is handled as adding a row level tombstone record in the SI family. This function reads
     * those values in for a given row and stores the tombstone timestamp so it can be used later to filter user data.
     */
    private Filter.ReturnCode processTombstone() throws IOException {
        rowState.setTombstoneTimestamp(keyValue.timestamp);
        return SKIP;
    }

    /**
     * Handle a cell that represents user data. Filter it based on the various timestamps and transaction status.
     */
    private Filter.ReturnCode processUserData() throws IOException {
        if (doneWithColumn()) {
            return NEXT_COL;
        } else {
            return filterUserDataByTimestamp();
        }
    }

    /**
     * Consider a cell of user data and decide whether to use it as "the" value for the column (in the context of the
     * current transaction) based on the various timestamp values and transaction status.
     */
    private Filter.ReturnCode filterUserDataByTimestamp() throws IOException {
        if (tombstoneAfterData()) {
            return NEXT_COL;
        } else if (isVisibleToCurrentTransaction()) {
            proceedToNextColumn();
            return INCLUDE;
        } else {
            return SKIP;
        }
    }

    /**
     * The version of HBase we are using does not support the newer "INCLUDE & NEXT_COL" return code
     * so this manually does the equivalent.
     */
    private void proceedToNextColumn() {
        rowState.lastValidQualifier = keyValue.qualifier;
    }

    /**
     * The second half of manually implementing our own "INCLUDE & NEXT_COL" return code.
     */
    private boolean doneWithColumn() {
        return dataLib.valuesEqual(keyValue.qualifier, rowState.lastValidQualifier);
    }

    /**
     * Is there a row level tombstone that supercedes the current cell?
     */
    private boolean tombstoneAfterData() {
        for (long tombstone : rowState.tombstoneTimestamps) {
            if (keyValue.timestamp < tombstone
                    || (keyValue.timestamp == tombstone && dataStore.isSiNull(keyValue.value))) {
                return true;
            }
            ;
        }
        return false;
    }

    /**
     * Should the current cell be visible to the current transaction? This is the core of the filtering of the data
     * under SI. It handles the various cases of when data should be visible.
     */
    private boolean isVisibleToCurrentTransaction() throws IOException {
        final Transaction transaction = loadTransaction();
        return (isDataCommittedBeforeThisTransaction(transaction)
                || isPartOfCurrentTransaction(transaction)
                || readCommitted(transaction)
                || readUncommitted(transaction));
    }

    /**
     * Retrieve the transaction for the current cell from the local cache.
     */
    private Transaction loadTransaction() throws IOException {
        final Transaction transaction = rowState.transactionCache.get(keyValue.timestamp);
        if (transaction == null) {
            throw new RuntimeException("All transactions should already be loaded from the si family for the data row, transaction Id: " + keyValue.timestamp);
        }
        return transaction;
    }

    private boolean isDataCommittedBeforeThisTransaction(Transaction dataTransaction) {
        return dataTransaction.isCommitted() && (dataTransaction.commitTimestamp < myTransaction.getRootBeginTimestamp());
    }

    private boolean isPartOfCurrentTransaction(Transaction dataTransaction) throws IOException {
        return dataTransaction.isVisiblePartOfTransaction(myTransaction);
    }

    private boolean readCommitted(Transaction dataTransaction) {
        return myTransaction.getEffectiveReadCommitted() && dataTransaction.isCommitted();
    }

    private boolean readUncommitted(Transaction dataTransaction) {
        return myTransaction.getEffectiveReadUncommitted() && dataTransaction.isActive();
    }

    /**
     * This is not expected to happen, but if there are extra unknown column families in the results they will be skipped.
     */
    private Filter.ReturnCode processUnknownFamilyData() {
        return SKIP;
    }

}
