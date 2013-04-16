package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.api.FilterState;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SiFilterState implements FilterState {
    final STable table;
    final ImmutableTransactionStruct immutableTransactionStruct;
    private final SDataLib dataLib;
    private final DataStore dataStore;
    private final TransactionStore transactionStore;

    Object currentRowKey;
    Map<Long, TransactionStruct> committedTransactions;
    Map<Long, TransactionStruct> stillRunningTransactions;
    Map<Long, TransactionStruct> failedTransactions;
    Object lastValidQualifier; // used to emulate the INCLUDE_AND_NEXT_COLUMN ReturnCode that is in later HBase versions
    Long tombstoneTimestamp = null;

    public SiFilterState(SDataLib dataLib, DataStore dataStore, TransactionStore transactionStore,
                         STable table, ImmutableTransactionStruct immutableTransactionStruct) {
        this.dataLib = dataLib;
        this.dataStore = dataStore;
        this.transactionStore = transactionStore;
        this.table = table;
        this.immutableTransactionStruct = immutableTransactionStruct;
        committedTransactions = new HashMap<Long, TransactionStruct>();
        stillRunningTransactions = new HashMap<Long, TransactionStruct>();
        failedTransactions = new HashMap<Long, TransactionStruct>();
    }

    public Filter.ReturnCode filterKeyValue(FilterState filterState, Object keyValue) throws IOException {
        SiFilterState siFilterState = (SiFilterState) filterState;
        Object rowKey = dataLib.getKeyValueRow(keyValue);
        if (siFilterState.currentRowKey == null || !dataLib.valuesEqual(siFilterState.currentRowKey, rowKey)) {
            siFilterState.currentRowKey = rowKey;
            siFilterState.lastValidQualifier = null;
        }
        if (dataStore.isCommitTimestampKeyValue(keyValue)) {
            filterProcessCommitTimestamp(keyValue, siFilterState);
            return Filter.ReturnCode.SKIP;
        } else if (dataStore.isTombstoneKeyValue(keyValue)) {
            if (filterKeepDataValue(keyValue, siFilterState)) {
                siFilterState.tombstoneTimestamp = dataLib.getKeyValueTimestamp(keyValue);
                return Filter.ReturnCode.NEXT_COL;
            }
        } else if (dataStore.isDataKeyValue(keyValue)) {
            if (dataLib.valuesEqual(dataLib.getKeyValueQualifier(keyValue), siFilterState.lastValidQualifier)) {
                return Filter.ReturnCode.NEXT_COL;
            } else {
                if (siFilterState.tombstoneTimestamp != null &&
                        dataLib.getKeyValueTimestamp(keyValue) < siFilterState.tombstoneTimestamp) {
                    return Filter.ReturnCode.NEXT_COL;
                } else if (filterKeepDataValue(keyValue, siFilterState)) {
                    siFilterState.lastValidQualifier = dataLib.getKeyValueQualifier(keyValue);
                    return Filter.ReturnCode.INCLUDE;
                }
            }
        }
        return Filter.ReturnCode.SKIP;
    }

    private void filterProcessCommitTimestamp(Object keyValue, SiFilterState siFilterState) throws IOException {
        long beginTimestamp = dataLib.getKeyValueTimestamp(keyValue);
        Object commitTimestampValue = dataLib.getKeyValueValue(keyValue);
        TransactionStruct transactionStruct = null;
        Boolean stillRunning = false;
        if (dataStore.isSiNull(commitTimestampValue)) {
            final Object[] temp = filterHandleUnknownTransactionStatus(siFilterState, keyValue, beginTimestamp);
            transactionStruct = (TransactionStruct) temp[0];
            stillRunning = (Boolean) temp[1];
        } else {
            transactionStruct = transactionStore.getTransactionStatus(beginTimestamp);
        }
        if (stillRunning) {
            siFilterState.stillRunningTransactions.put(beginTimestamp, transactionStruct);
        } else if (transactionStruct.commitTimestamp == null) {
            siFilterState.failedTransactions.put(beginTimestamp, transactionStruct);
        } else {
            siFilterState.committedTransactions.put(beginTimestamp, transactionStruct);
        }
    }

    private Object[] filterHandleUnknownTransactionStatus(SiFilterState siFilterState, Object keyValue,
                                                          long beginTimestamp) throws IOException {
        TransactionStruct transactionStruct = transactionStore.getTransactionStatus(beginTimestamp);
        Boolean stillRunning = !transactionStruct.isCacheable();
        switch (transactionStruct.getEffectiveStatus()) {
            case ACTIVE:
                stillRunning = true;
                break;
            case ERROR:
            case ROLLED_BACK:
                cleanupErrors();
                break;
            case COMMITTING:
                //TODO: needs special handling
                stillRunning = true;
                break;
            case COMMITED:
                rollForward(siFilterState, keyValue, transactionStruct);
                break;
        }
        return new Object[]{transactionStruct, stillRunning};
    }

    private void rollForward(SiFilterState siFilterState, Object keyValue, TransactionStruct transactionStruct) {
        if (transactionStruct.parent == null || !transactionStruct.dependent) {
            dataStore.setCommitTimestamp(siFilterState.table, keyValue, transactionStruct.beginTimestamp, transactionStruct.commitTimestamp);
        }
    }

    private boolean filterKeepDataValue(Object keyValue, SiFilterState siFilterState) throws IOException {
        long dataTimestamp = dataLib.getKeyValueTimestamp(keyValue);
        updateFilterState(siFilterState, dataTimestamp);
        Long commitTimestamp = siFilterState.committedTransactions.containsKey(dataTimestamp)
                ? siFilterState.committedTransactions.get(dataTimestamp).commitTimestamp
                : null;
        Boolean stillRunning = siFilterState.stillRunningTransactions.containsKey(dataTimestamp);
        if (isCommittedBeforeThisTransaction(siFilterState, commitTimestamp)
                || isThisTransactionsData(siFilterState, dataTimestamp)
                || readCommittedAndCommitted(siFilterState, commitTimestamp)
                || readDirtyAndActive(siFilterState, stillRunning)) {
            return true;
        }
        return false;
    }

    private void updateFilterState(SiFilterState siFilterState, long beginTimestamp) throws IOException {
        TransactionStruct cachedTransaction = getTransactionFromFilterState(siFilterState, beginTimestamp);
        if (cachedTransaction == null) {
            final TransactionStruct transactionStruct = transactionStore.getTransactionStatus(beginTimestamp);
            boolean stillRunning = !transactionStruct.isCacheable();
            if (stillRunning) {
                siFilterState.stillRunningTransactions.put(beginTimestamp, transactionStruct);
            } else if (transactionStruct.commitTimestamp == null) {
                siFilterState.failedTransactions.put(beginTimestamp, transactionStruct);
            } else {
                siFilterState.committedTransactions.put(beginTimestamp, transactionStruct);
            }
        }
    }

    private TransactionStruct getTransactionFromFilterState(SiFilterState siFilterState, long beginTimestamp) {
        TransactionStruct cachedTransaction = siFilterState.committedTransactions.get(beginTimestamp);
        if (cachedTransaction == null) {
            cachedTransaction = siFilterState.failedTransactions.get(beginTimestamp);
        }
        if (cachedTransaction == null) {
            cachedTransaction = siFilterState.stillRunningTransactions.get(beginTimestamp);
        }
        return cachedTransaction;
    }

    private boolean readDirtyAndActive(SiFilterState siFilterState, Boolean stillRunning) {
        return siFilterState.immutableTransactionStruct.getEffectiveReadUncommitted() && stillRunning;
    }

    private boolean readCommittedAndCommitted(SiFilterState siFilterState, Long commitTimestamp) {
        return siFilterState.immutableTransactionStruct.getEffectiveReadCommitted() && (commitTimestamp != null);
    }

    private boolean isCommittedBeforeThisTransaction(SiFilterState siFilterState, Long commitTimestamp) {
        return (commitTimestamp != null && commitTimestamp < siFilterState.immutableTransactionStruct.getRootBeginTimestamp());
    }

    private boolean isThisTransactionsData(SiFilterState siFilterState, long dataTimestamp)
            throws IOException {
        final TransactionStruct dataTransaction = getTransactionFromFilterState(siFilterState, dataTimestamp);
        final TransactionStatus dataStatus = dataTransaction.getEffectiveStatus();
        return ((dataStatus == TransactionStatus.ACTIVE
                || dataStatus == TransactionStatus.COMMITTING
                || dataStatus == TransactionStatus.COMMITED)
                && dataTransaction.getRootBeginTimestamp() == siFilterState.immutableTransactionStruct.getRootBeginTimestamp());
    }

    private void cleanupErrors() {
        //TODO: implement this
    }

}
