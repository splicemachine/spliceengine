package com.splicemachine.si.impl;

import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.data.api.SDataLib;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.hbase.filter.Filter.ReturnCode.INCLUDE;
import static org.apache.hadoop.hbase.filter.Filter.ReturnCode.NEXT_COL;
import static org.apache.hadoop.hbase.filter.Filter.ReturnCode.SKIP;

/**
 * Contains the logic for performing an HBase-style filter using "snapshot isolation" logic. This means it filters out
 * data that should not be seen by the transaction that is performing the read operation (either a "get" or a "scan").
 */
public class FilterState<Data, Result, KeyValue, OperationWithAttributes, Put extends OperationWithAttributes, Delete, Get extends OperationWithAttributes,
        Scan, Lock, OperationStatus, Hashable extends Comparable, Mutation, IHTable, Scanner>
        implements IFilterState<KeyValue> {
    static final Logger LOG = Logger.getLogger(FilterState.class);

    /**
     * The transactions that have been loaded as part of running this filter.
     */
    final Map<Long, Transaction> transactionCache;
    final Map<Long, VisibleResult> visibleCache;

    private final ImmutableTransaction myTransaction;
    private final SDataLib<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock, OperationStatus> dataLib;
    private final DataStore<Data, Hashable, Result, KeyValue, OperationWithAttributes, Mutation, Put,
            Delete, Get, Scan, IHTable, Lock, OperationStatus, Scanner> dataStore;
    private final TransactionStore transactionStore;
    private final RollForwardQueue rollForwardQueue;
    private final boolean includeSIColumn;
    private final boolean includeUncommittedAsOfStart;
    private boolean ignoreDoneWithColumn;
    
    private final FilterRowState<Data, Result, KeyValue, Put, Delete, Get, Scan, OperationWithAttributes, Lock, OperationStatus> rowState;
    final DecodedKeyValue<Data, Result, KeyValue, Put, Delete, Get, Scan, OperationWithAttributes, Lock, OperationStatus> keyValue;
    KeyValueType type;

    private final TransactionSource transactionSource;

    FilterState(SDataLib dataLib, DataStore dataStore, TransactionStore transactionStore,
                RollForwardQueue rollForwardQueue, boolean includeSIColumn, boolean includeUncommittedAsOfStart,
                ImmutableTransaction myTransaction) {
        this.transactionSource = new TransactionSource() {
            @Override
            public Transaction getTransaction(long timestamp) throws IOException {
                return getTransactionFromFilterCache(timestamp);
            }
        };
        this.dataLib = dataLib;
        this.keyValue = new DecodedKeyValue(dataLib);
        this.dataStore = dataStore;
        this.transactionStore = transactionStore;
        this.rollForwardQueue = rollForwardQueue;
        this.includeSIColumn = includeSIColumn;
        this.includeUncommittedAsOfStart = includeUncommittedAsOfStart;
        this.myTransaction = myTransaction;

        transactionCache = CacheMap.makeCache(false);
        visibleCache = CacheMap.makeCache(false);

        // initialize internal state
        this.rowState = new FilterRowState(dataLib);
    }

    public void setIgnoreDoneWithColumn() {
        this.ignoreDoneWithColumn = true;
    }

    /**
     * The public entry point. This returns an HBase filter code and is expected to serve as the heart of an HBase filter
     * implementation.
     * The order of the column families is important. It is expected that the SI family will be processed first.
     */
    @Override
    public Filter.ReturnCode filterKeyValue(KeyValue dataKeyValue) throws IOException {
        setKeyValue(dataKeyValue);
        return filterByColumnType();
    }

    void setKeyValue(KeyValue dataKeyValue) {
        keyValue.setKeyValue(dataKeyValue);
        rowState.updateCurrentRow(keyValue);
        type = dataStore.getKeyValueType(keyValue.keyValue());
    }

    @Override
    public void nextRow() {
        rowState.resetCurrentRow();
    }

    /**
     * Look at the column family and qualifier to determine how to "dispatch" the current keyValue.
     */
    Filter.ReturnCode filterByColumnType() throws IOException {
        if (rowState.inData) {
            return processUserDataShortCircuit();
        }
        if (type.equals(KeyValueType.TOMBSTONE)) {
            return processTombstone();
        } else if (type.equals(KeyValueType.ANTI_TOMBSTONE)) {
            return processAntiTombstone();
        } else if (type.equals(KeyValueType.COMMIT_TIMESTAMP)) {
            if (includeSIColumn && !rowState.isSiColumnIncluded()) {
                processCommitTimestamp();
                return processCommitTimestampAsUserData();
            } else {
                return processCommitTimestamp();
            }
        } else if (type.equals(KeyValueType.USER_DATA)) {
            return processUserDataSetupShortCircuit();
        } else {
            return processUnknownFamilyData();
        }
    }

    private Filter.ReturnCode processUserDataSetupShortCircuit() throws IOException {
        log("processUserDataSetupShortCircuit");
        final Filter.ReturnCode result = processUserData();
        rowState.inData = true;
        if (rowState.transactionCache.size() == 1) {
            rowState.shortCircuit = result;
        }
        return result;
    }

    private Filter.ReturnCode processUserDataShortCircuit() throws IOException {
        if (rowState.shortCircuit != null) {
            return rowState.shortCircuit;
        } else {
            return processUserData();
        }
    }

    private Filter.ReturnCode processCommitTimestampAsUserData() throws IOException {
        log("processCommitTimestampAsUserData");
        boolean later = false;
        for (Long tombstoneTimestamp : rowState.tombstoneTimestamps) {
            if (tombstoneTimestamp < keyValue.timestamp()) {
                later = true;
                break;
            }
        }
        if (later) {
            rowState.rememberCommitTimestamp(keyValue.keyValue());
            return SKIP;
        } else {
            final KeyValue oldKeyValue = keyValue.keyValue();
            boolean include = false;
            for (KeyValue kv : rowState.getCommitTimestamps()) {
                keyValue.setKeyValue(kv);
                if (processUserData().equals(INCLUDE)) {
                    include = true;
                    break;
                }
            }
            keyValue.setKeyValue(oldKeyValue);
            if (include) {
                rowState.setSiColumnIncluded();
                return INCLUDE;
            } else {
                final Filter.ReturnCode returnCode = processUserData();
                if (returnCode.equals(SKIP)) {
                } else {
                    rowState.setSiColumnIncluded();
                }
                return returnCode;
            }
        }
    }

    /**
     * Handles the "commit timestamp" values that SI writes to each data row. Processing these values causes the
     * relevant transaction data to be loaded up into the local cache.
     */
    private Filter.ReturnCode processCommitTimestamp() throws IOException {
        log("processCommitTimestamp");
        Transaction transaction = transactionCache.get(keyValue.timestamp());
        if (transaction == null) {
            transaction = processCommitTimestampDirect();
        }
        rowState.transactionCache.put(transaction.getTransactionId().getId(), transaction);
        return SKIP;
    }

    private Transaction processCommitTimestampDirect() throws IOException {
        Transaction transaction;
        if (dataStore.isSINull(keyValue.keyValue())) {
            transaction = handleUnknownTransactionStatus();
        } else if (dataStore.isSIFail(keyValue.keyValue())) {
            transaction = transactionStore.makeStubFailedTransaction(keyValue.timestamp());
            transactionCache.put(keyValue.timestamp(), transaction);
        } else {
            transaction = transactionStore.makeStubCommittedTransaction(keyValue.timestamp(), (Long) dataLib.decode(keyValue.value(), Long.class));
            transactionCache.put(keyValue.timestamp(), transaction);
        }
        return transaction;
    }

    private Transaction getTransactionFromFilterCache(long timestamp) throws IOException {
        Transaction transaction = transactionCache.get(timestamp);
        if (transaction == null) {
            if (!myTransaction.getEffectiveReadCommitted() && !myTransaction.getEffectiveReadUncommitted()
                    && !myTransaction.isAncestorOf(transactionStore.getImmutableTransaction(timestamp))) {
                transaction = transactionStore.getTransactionAsOf(timestamp, myTransaction.getLongTransactionId());
            } else {
                transaction = transactionStore.getTransaction(timestamp);
            }
            transactionCache.put(transaction.getTransactionId().getId(), transaction);
        }
        return transaction;
    }

    /**
     * Handles the case where the commit timestamp cell contains a begin timestamp, but doesn't have an
     * associated commit timestamp in the cell. This means the transaction table needs to be consulted to find out
     * the current status of the transaction.
     */
    private Transaction handleUnknownTransactionStatus() throws IOException {
        final Transaction cachedTransaction = rowState.transactionCache.get(keyValue.timestamp());
        if (cachedTransaction == null) {
            final Transaction dataTransaction = getTransactionFromFilterCache(keyValue.timestamp());
            final VisibleResult visibleResult = checkVisibility(dataTransaction);
            if (visibleResult.effectiveStatus.isFinished()) {
                rollForward(dataTransaction);
            }
            return dataTransaction;
        } else {
            return cachedTransaction;
        }
    }

    private VisibleResult checkVisibility(Transaction dataTransaction) throws IOException {
        final long timestamp = dataTransaction.getLongTransactionId();
        VisibleResult result = visibleCache.get(timestamp);
        if (result == null) {
            result = myTransaction.canSee(dataTransaction, transactionSource);
            visibleCache.put(timestamp, result);
        }
        return result;
    }

    /**
     * Update the data row to remember the commit timestamp of the transaction. This avoids the need to look the
     * transaction up in the transaction table again the next time this row is read.
     */
    private void rollForward(Transaction transaction) throws IOException {
        TransactionStatus status = transaction.getEffectiveStatus();
        if (rollForwardQueue != null && status.isFinished()) {
            // TODO: revisit this in light of nested independent transactions
            dataStore.recordRollForward(rollForwardQueue, transaction.getLongTransactionId(), keyValue.row(), true);
        }
    }

    /**
     * Under SI, deleting a row is handled as adding a row level tombstone record in the SI family. This function reads
     * those values in for a given row and stores the tombstone timestamp so it can be used later to filter user data.
     */
    private Filter.ReturnCode processTombstone() throws IOException {
        log("processTombstone");
        if (rowState.setTombstoneTimestamp(keyValue.timestamp()) && includeUncommittedAsOfStart
                && !rowState.isSiTombstoneIncluded() && keyValue.timestamp() < myTransaction.getTransactionId().getId()) {
            rowState.setSiTombstoneIncluded();
            return INCLUDE;
        } else {
            return SKIP;
        }
    }

    private Filter.ReturnCode processAntiTombstone() throws IOException {
        log("processAntiTombstone");
        rowState.setAntiTombstoneTimestamp(keyValue.timestamp());
        return SKIP;
    }

    /**
     * Handle a cell that represents user data. Filter it based on the various timestamps and transaction status.
     */
    private Filter.ReturnCode processUserData() throws IOException {
        if (doneWithColumn() && !ignoreDoneWithColumn) {
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
            if (includeUncommittedAsOfStart) {
                if (isUncommittedAsOfStart()) {
                    return INCLUDE;
                } else {
                    return SKIP;
                }
            } else {
                return SKIP;
            }
        }
    }

    private boolean isUncommittedAsOfStart() throws IOException {
        return myTransaction.startedWhileOtherActive(loadTransaction(), transactionSource);
    }

    /**
     * The version of HBase we are using does not support the newer "INCLUDE & NEXT_COL" return code
     * so this manually does the equivalent.
     */
    private void proceedToNextColumn() {
        rowState.lastValidQualifier = keyValue.keyValue();
    }

    /**
     * The second half of manually implementing our own "INCLUDE & NEXT_COL" return code.
     */
    private boolean doneWithColumn() {
        return dataLib.matchingQualifierKeyValue(keyValue.keyValue(), rowState.lastValidQualifier);
    }

    /**
     * Is there a row level tombstone that supercedes the current cell?
     */
    private boolean tombstoneAfterData() throws IOException {
        for (long tombstone : rowState.tombstoneTimestamps) {
            final Transaction tombstoneTransaction = rowState.transactionCache.get(tombstone);
            final VisibleResult visibleResult = checkVisibility(tombstoneTransaction);
            if (visibleResult.visible && (keyValue.timestamp() <= tombstone)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Should the current cell be visible to the current transaction? This is the core of the filtering of the data
     * under SI. It handles the various cases of when data should be visible.
     */
    private boolean isVisibleToCurrentTransaction() throws IOException {
        if (keyValue.timestamp() == myTransaction.getTransactionId().getId() && !myTransaction.getTransactionId().independentReadOnly) {
            return true;
        } else {
            final Transaction transaction = loadTransaction();
            return checkVisibility(transaction).visible;
        }
    }

    /**
     * Retrieve the transaction for the current cell from the local cache.
     */
    private Transaction loadTransaction() throws IOException {
        final Transaction transaction = rowState.transactionCache.get(keyValue.timestamp());
        if (transaction == null) {
            throw new RuntimeException("All transactions should already be loaded from the si family for the data row, transaction Id: " + keyValue.timestamp());
        }
        return transaction;
    }

    /**
     * This is not expected to happen, but if there are extra unknown column families in the results they will be skipped.
     */
    private Filter.ReturnCode processUnknownFamilyData() {
        log("processUnknownFamilyData");
        return SKIP;
    }

    private void log(String message) {
        //System.out.println("  " + message);
    }

    @Override
    public KeyValue produceAccumulatedKeyValue() {
        return null;
    }

    @Override
    public boolean getExcludeRow() {
        return false;
    }
}
