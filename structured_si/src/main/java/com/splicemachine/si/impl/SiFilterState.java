package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.api.FilterState;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SiFilterState implements FilterState {
    private final STable table;
    private final ImmutableTransactionStruct myTransaction;
    private final SDataLib dataLib;
    private final DataStore dataStore;
    private final TransactionStore transactionStore;

    private final Map<Long, TransactionStruct> committedTransactions;
    private final Map<Long, TransactionStruct> stillRunningTransactions;
    private final Map<Long, TransactionStruct> failedTransactions;

    private FilterRowState rowState;

    public SiFilterState(SDataLib dataLib, DataStore dataStore, TransactionStore transactionStore,
                         STable table, ImmutableTransactionStruct myTransaction) {
        this.dataLib = dataLib;
        this.dataStore = dataStore;
        this.transactionStore = transactionStore;
        this.table = table;
        this.myTransaction = myTransaction;
        committedTransactions = new HashMap<Long, TransactionStruct>();
        stillRunningTransactions = new HashMap<Long, TransactionStruct>();
        failedTransactions = new HashMap<Long, TransactionStruct>();
        rowState = new FilterRowState(dataLib);
    }

    public Filter.ReturnCode filterKeyValue(Object dataKeyValue) throws IOException {
        Object rowKey = dataLib.getKeyValueRow(dataKeyValue);
        rowState.updateCurrentRow(rowKey);
        if (dataStore.isCommitTimestampKeyValue(dataKeyValue)) {
            filterProcessCommitTimestamp(dataKeyValue);
            return Filter.ReturnCode.SKIP;
        } else if (dataStore.isTombstoneKeyValue(dataKeyValue)) {
            if (filterKeepDataValue(dataKeyValue)) {
                rowState.setTombstoneTimestamp(dataLib.getKeyValueTimestamp(dataKeyValue));
                return Filter.ReturnCode.NEXT_COL;
            }
        } else if (dataStore.isDataKeyValue(dataKeyValue)) {
            if (dataLib.valuesEqual(dataLib.getKeyValueQualifier(dataKeyValue), rowState.lastValidQualifier)) {
                return Filter.ReturnCode.NEXT_COL;
            } else {
                if (rowState.tombstoneTimestamp != null &&
                        dataLib.getKeyValueTimestamp(dataKeyValue) < rowState.tombstoneTimestamp) {
                    return Filter.ReturnCode.NEXT_COL;
                } else if (filterKeepDataValue(dataKeyValue)) {
                    rowState.lastValidQualifier = dataLib.getKeyValueQualifier(dataKeyValue);
                    return Filter.ReturnCode.INCLUDE;
                }
            }
        }
        return Filter.ReturnCode.SKIP;
    }

    private void filterProcessCommitTimestamp(Object keyValue) throws IOException {
        long beginTimestamp = dataLib.getKeyValueTimestamp(keyValue);
        Object commitTimestampValue = dataLib.getKeyValueValue(keyValue);
        TransactionStruct transactionStruct = null;
        Boolean stillRunning = false;
        if (dataStore.isSiNull(commitTimestampValue)) {
            final Object[] temp = filterHandleUnknownTransactionStatus(keyValue, beginTimestamp);
            transactionStruct = (TransactionStruct) temp[0];
            stillRunning = (Boolean) temp[1];
        } else {
            transactionStruct = transactionStore.getTransactionStatus(beginTimestamp);
        }
        if (stillRunning) {
            stillRunningTransactions.put(beginTimestamp, transactionStruct);
        } else if (transactionStruct.didNotCommit()) {
            failedTransactions.put(beginTimestamp, transactionStruct);
        } else {
            committedTransactions.put(beginTimestamp, transactionStruct);
        }
    }

    private Object[] filterHandleUnknownTransactionStatus(Object keyValue,
                                                          long beginTimestamp) throws IOException {
        TransactionStruct transactionStruct = transactionStore.getTransactionStatus(beginTimestamp);
        Boolean stillRunning = transactionStruct.isStillRunning();
        switch (transactionStruct.getEffectiveStatus()) {
            case ACTIVE:
                break;
            case ERROR:
            case ROLLED_BACK:
                cleanupErrors();
                break;
            case COMMITTING:
                //TODO: needs special handling
                break;
            case COMMITED:
                rollForward(keyValue, transactionStruct);
                break;
        }
        return new Object[]{transactionStruct, stillRunning};
    }

    private void rollForward(Object keyValue, TransactionStruct transactionStruct) {
        if (transactionStruct.parent == null || !transactionStruct.dependent) {
            dataStore.setCommitTimestamp(table, keyValue, transactionStruct.beginTimestamp, transactionStruct.commitTimestamp);
        }
    }

    private boolean filterKeepDataValue(Object dataKeyValue) throws IOException {
        long dataTimestamp = dataLib.getKeyValueTimestamp(dataKeyValue);
        updateFilterState(dataTimestamp);
        TransactionStruct committedDataTransaction = committedTransactions.get(dataTimestamp);
        Boolean dataTransactionStillRunning = stillRunningTransactions.containsKey(dataTimestamp);
        if (isDataCommittedBeforeThisTransaction(committedDataTransaction)
                || isThisTransactionsData(dataTimestamp)
                || readCommittedAndDataTransactionCommitted(committedDataTransaction)
                || readDirtyAndDataTransactionStillRunning(dataTransactionStillRunning)) {
            return true;
        }
        return false;
    }

    private void updateFilterState(long beginTimestamp) throws IOException {
        TransactionStruct cachedTransaction = getTransactionFromFilterState(beginTimestamp);
        if (cachedTransaction == null) {
            final TransactionStruct transactionStruct = transactionStore.getTransactionStatus(beginTimestamp);
            if (transactionStruct.isStillRunning()) {
                stillRunningTransactions.put(beginTimestamp, transactionStruct);
            } else if (transactionStruct.didNotCommit()) {
                failedTransactions.put(beginTimestamp, transactionStruct);
            } else {
                committedTransactions.put(beginTimestamp, transactionStruct);
            }
        }
    }

    private TransactionStruct getTransactionFromFilterState(long beginTimestamp) {
        TransactionStruct cachedTransaction = committedTransactions.get(beginTimestamp);
        if (cachedTransaction == null) {
            cachedTransaction = failedTransactions.get(beginTimestamp);
        }
        if (cachedTransaction == null) {
            cachedTransaction = stillRunningTransactions.get(beginTimestamp);
        }
        return cachedTransaction;
    }

    private boolean readDirtyAndDataTransactionStillRunning(Boolean stillRunning) {
        return myTransaction.getEffectiveReadUncommitted() && stillRunning;
    }

    private boolean readCommittedAndDataTransactionCommitted(TransactionStruct dataTransaction) {
        if (dataTransaction != null) {
            return myTransaction.getEffectiveReadCommitted() && (dataTransaction.commitTimestamp != null);
        }
        return false;
    }

    private boolean isDataCommittedBeforeThisTransaction(TransactionStruct dataTransaction) {
        if (dataTransaction != null) {
            Long dataTimestamp = dataTransaction.commitTimestamp;
            return (dataTimestamp != null && dataTimestamp < myTransaction.getRootBeginTimestamp());
        }
        return false;
    }

    private boolean isThisTransactionsData(long dataTimestamp)
            throws IOException {
        final TransactionStruct dataTransaction = getTransactionFromFilterState(dataTimestamp);
        final TransactionStatus dataStatus = dataTransaction.getEffectiveStatus();
        return ((dataStatus == TransactionStatus.ACTIVE
                || dataStatus == TransactionStatus.COMMITTING
                || dataStatus == TransactionStatus.COMMITED)
                && dataTransaction.getRootBeginTimestamp() == myTransaction.getRootBeginTimestamp());
    }

    private void cleanupErrors() {
        //TODO: implement this
    }

}
