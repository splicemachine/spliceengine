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
    private final ImmutableTransaction myTransaction;
    private final SDataLib dataLib;
    private final DataStore dataStore;
    private final TransactionStore transactionStore;

    private final Map<Long, Transaction> committedTransactions;
    private final Map<Long, Transaction> stillRunningTransactions;
    private final Map<Long, Transaction> failedTransactions;

    private FilterRowState rowState;

    public SiFilterState(SDataLib dataLib, DataStore dataStore, TransactionStore transactionStore,
                         STable table, ImmutableTransaction myTransaction) {
        this.dataLib = dataLib;
        this.dataStore = dataStore;
        this.transactionStore = transactionStore;
        this.table = table;
        this.myTransaction = myTransaction;
        committedTransactions = new HashMap<Long, Transaction>();
        stillRunningTransactions = new HashMap<Long, Transaction>();
        failedTransactions = new HashMap<Long, Transaction>();
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
        Transaction transaction = null;
        Boolean stillRunning = false;
        if (dataStore.isSiNull(commitTimestampValue)) {
            final Object[] temp = filterHandleUnknownTransactionStatus(keyValue, beginTimestamp);
            transaction = (Transaction) temp[0];
            stillRunning = (Boolean) temp[1];
        } else {
            transaction = transactionStore.getTransaction(beginTimestamp);
        }
        if (stillRunning) {
            stillRunningTransactions.put(beginTimestamp, transaction);
        } else if (!transaction.isCommitted()) {
            failedTransactions.put(beginTimestamp, transaction);
        } else {
            committedTransactions.put(beginTimestamp, transaction);
        }
    }

    private Object[] filterHandleUnknownTransactionStatus(Object keyValue,
                                                          long beginTimestamp) throws IOException {
        Transaction transaction = transactionStore.getTransaction(beginTimestamp);
        if (transaction.isCommitted()) {
            rollForward(keyValue, transaction);
        } else if (transaction.isFailed()) {
            cleanupErrors();
        } else if (transaction.isCommitting()) {
            //TODO: needs special handling
        }
        return new Object[]{transaction, transaction.isActive()};
    }

    private void rollForward(Object keyValue, Transaction transaction) {
        if (!transaction.isNestedDependent()) {
            dataStore.setCommitTimestamp(table, keyValue, transaction.beginTimestamp, transaction.commitTimestamp);
        }
    }

    private boolean filterKeepDataValue(Object dataKeyValue) throws IOException {
        long dataTimestamp = dataLib.getKeyValueTimestamp(dataKeyValue);
        assertTransactionInFilterState(dataTimestamp);
        Transaction committedDataTransaction = committedTransactions.get(dataTimestamp);
        Boolean dataTransactionStillRunning = stillRunningTransactions.containsKey(dataTimestamp);
        if (isDataCommittedBeforeThisTransaction(committedDataTransaction)
                || isThisTransactionsData(dataTimestamp)
                || readCommittedAndDataTransactionCommitted(committedDataTransaction)
                || readDirtyAndDataTransactionStillRunning(dataTransactionStillRunning)) {
            return true;
        }
        return false;
    }

    private void assertTransactionInFilterState(long beginTimestamp) throws IOException {
        if (getTransactionFromFilterState(beginTimestamp) == null) {
            throw new RuntimeException("All transactions should already be loaded from the si family for the data row");
        }
    }

    private Transaction getTransactionFromFilterState(long beginTimestamp) {
        Transaction cachedTransaction = committedTransactions.get(beginTimestamp);
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

    private boolean readCommittedAndDataTransactionCommitted(Transaction dataTransaction) {
        if (dataTransaction == null) {
            return false;
        }
        return myTransaction.getEffectiveReadCommitted() && dataTransaction.isCommitted();
    }

    private boolean isDataCommittedBeforeThisTransaction(Transaction dataTransaction) {
        if (dataTransaction == null) {
            return false;
        }
        return dataTransaction.committedBefore(myTransaction);
    }

    private boolean isThisTransactionsData(long dataTimestamp) throws IOException {
        return getTransactionFromFilterState(dataTimestamp).isVisiblePartOfTransaction(myTransaction);
    }

    private void cleanupErrors() {
        //TODO: implement this
    }

}
