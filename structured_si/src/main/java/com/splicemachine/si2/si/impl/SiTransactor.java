package com.splicemachine.si2.si.impl;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.SRowLock;
import com.splicemachine.si2.data.api.SScan;
import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.data.api.STableWriter;
import com.splicemachine.si2.si.api.ClientTransactor;
import com.splicemachine.si2.si.api.FilterState;
import com.splicemachine.si2.si.api.TimestampSource;
import com.splicemachine.si2.si.api.TransactionId;
import com.splicemachine.si2.si.api.Transactor;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SiTransactor implements Transactor, ClientTransactor {
    static final Logger LOG = Logger.getLogger(SiTransactor.class);

    private final TimestampSource timestampSource;
    private final SDataLib dataLib;
    private final STableWriter dataWriter;
    private final DataStore dataStore;
    private final TransactionStore transactionStore;

    public SiTransactor(TimestampSource timestampSource, SDataLib dataLib, STableWriter dataWriter,
                        DataStore dataStore, TransactionStore transactionStore) {
        this.timestampSource = timestampSource;
        this.dataLib = dataLib;
        this.dataWriter = dataWriter;
        this.dataStore = dataStore;
        this.transactionStore = transactionStore;
    }

    @Override
    public TransactionId beginTransaction(boolean allowWrites) throws IOException {
        final SiTransactionId transactionId = new SiTransactionId(timestampSource.nextTimestamp());
        transactionStore.recordNewTransaction(transactionId, allowWrites, TransactionStatus.ACTIVE);
        return transactionId;
    }

    @Override
    public TransactionId transactionIdFromString(String transactionId) {
        return new SiTransactionId(Long.valueOf(transactionId));
    }

    @Override
    public TransactionId getTransactionIdFromPut(Object put) {
        return dataStore.getTransactionIdFromOperation(put);
    }

    @Override
    public TransactionId getTransactionIdFromDelete(Delete delete) {
        return dataStore.getTransactionIdFromOperation(delete);
    }

    @Override
    public void commit(TransactionId transactionId) throws IOException {
        ensureTransactionActive(transactionId);
        transactionStore.recordTransactionStatusChange(transactionId, TransactionStatus.COMMITTING);
        final long endId = timestampSource.nextTimestamp();
        transactionStore.recordTransactionCommit(transactionId, endId, TransactionStatus.COMMITED);
    }

    @Override
    public void abort(TransactionId transactionId) throws IOException {
        TransactionStruct transaction = transactionStore.getTransactionStatus(transactionId);
        if (transaction.status.equals(TransactionStatus.ACTIVE)) {
            transactionStore.recordTransactionStatusChange(transactionId, TransactionStatus.ABORT);
        }
    }

    @Override
    public void fail(TransactionId transactionId) throws IOException {
        ensureTransactionActive(transactionId);
        transactionStore.recordTransactionStatusChange(transactionId, TransactionStatus.ERROR);
    }

    @Override
    public void initializeGet(TransactionId transactionId, SGet get) {
        dataStore.setSiNeededAttribute(get);
        dataLib.setGetTimeRange(get, 0, transactionId.getId() + 1);
        dataLib.setGetMaxVersions(get);
    }

    @Override
    public void initializeGets(TransactionId transactionId, List gets) {
        for (Object get : gets) {
            dataStore.setSiNeededAttribute(get);
            dataLib.setGetTimeRange((SGet) get, 0L, transactionId.getId() + 1);
        }
    }

    @Override
    public void initializeScan(TransactionId transactionId, SScan scan) {
        dataStore.setSiNeededAttribute(scan);
        dataLib.setScanTimeRange(scan, 0L, transactionId.getId() + 1);
        dataLib.setScanMaxVersions(scan);
    }

    @Override
    public void initializePut(TransactionId transactionId, Object put) {
        dataStore.setSiNeededAttribute(put);
        dataStore.setTransactionId((SiTransactionId) transactionId, put);
    }

    @Override
    public void initializePut(Object put1, Object put2) {
        if (dataStore.getSiNeededAttribute(put1)) {
            final SiTransactionId transactionId = dataStore.getTransactionIdFromOperation(put1);
            initializePut(transactionId, put2);
        }
    }

    @Override
    public void initializeGetFromDelete(Object delete, Object put) {
        if (dataStore.getSiNeededAttribute(delete)) {
            final SiTransactionId transactionId = dataStore.getTransactionIdFromOperation(delete);
            initializePut(transactionId, put);
        }
    }

    @Override
    public Object newDeletePut(TransactionId transactionId, Object rowKey) {
        SiTransactionId siTransactionId = (SiTransactionId) transactionId;
        final Object deletePut = dataLib.newPut(rowKey);
        initializePut(siTransactionId, deletePut);
        dataStore.setTombstoneOnPut(deletePut, siTransactionId);
        dataStore.setDeletePutAttribute(deletePut);
        return deletePut;
    }

    @Override
    public boolean isDeletePut(Object put) {
        final Boolean deleteAttribute = dataStore.getDeletePutAttribute(put);
        return (deleteAttribute != null && deleteAttribute);
    }

    @Override
    public boolean processPut(STable table, Object put) throws IOException {
        Boolean siNeeded = dataStore.getSiNeededAttribute(put);
        if (siNeeded != null && siNeeded) {
            SiTransactionId transactionId = dataStore.getTransactionIdFromOperation(put);
            ensureTransactionAllowsWrites(transactionId);
            Object rowKey = dataLib.getPutKey(put);
            SRowLock lock = dataWriter.lockRow(table, rowKey);
            try {
                checkForConflict(transactionId, table, lock, rowKey);
                Object newPut = dataStore.newLockWithPut(transactionId, put, lock);
                dataStore.addTransactionIdToPut(newPut, transactionId);
                dataWriter.write(table, newPut, lock);
            } finally {
                dataWriter.unLockRow(table, lock);
            }
            return true;
        } else {
            return false;
        }
    }

    private void checkForConflict(TransactionId transactionId, STable table, SRowLock lock, Object rowKey) throws IOException {
        long id = transactionId.getId();
        List keyValues = dataStore.getCommitTimestamp(table, rowKey);
        if (keyValues != null) {
            int index = 0;
            boolean loop = true;
            while (loop) {
                if (index >= keyValues.size()) {
                    loop = false;
                } else {
                    Object c = keyValues.get(index);
                    long cellTimestamp = dataLib.getKeyValueTimestamp(c);
                    TransactionStruct transaction = transactionStore.getTransactionStatus(cellTimestamp);
                    if (transaction.status.equals(TransactionStatus.COMMITED)) {
                        if (transaction.commitTimestamp > id) {
                            writeWriteConflict(transactionId);
                        }
                    } else if (transaction.status.equals(TransactionStatus.ACTIVE) || transaction.status.equals(TransactionStatus.COMMITTING)) {
                        // if the KeyValue was written by the current running transaction then it is not a conflict
                        if (transaction.beginTimestamp != id) {
                            writeWriteConflict(transactionId);
                        }
                    }
                    index++;
                }
            }
        }
    }

    private void writeWriteConflict(TransactionId transactionId) throws IOException {
        fail(transactionId);
        throw new DoNotRetryIOException("write/write conflict");
    }

    @Override
    public Object filterResult(FilterState filterState, Object result) throws IOException {
        SiFilterState siFilterState = (SiFilterState) filterState;
        ensureTransactionActive(siFilterState.transactionId);

        List<Object> filteredCells = new ArrayList<Object>();
        final List keyValues = dataLib.listResult(result);
        if (keyValues != null) {
            Object qualifierToSkip = null;
            Object familyToSkip = null;

            for (Object keyValue : keyValues) {
                if (familyToSkip != null
                        && dataLib.valuesEqual(familyToSkip, dataLib.getKeyValueFamily(keyValue))
                        && dataLib.valuesEqual(qualifierToSkip, dataLib.getKeyValueQualifier(keyValue))) {
                    // skipping to next column
                } else {
                    familyToSkip = null;
                    qualifierToSkip = null;
                    Filter.ReturnCode returnCode = filterKeyValue(filterState, keyValue);
                    switch (returnCode) {
                        case SKIP:
                            break;
                        case INCLUDE:
                            filteredCells.add(keyValue);
                            qualifierToSkip = dataLib.getKeyValueQualifier(keyValue);
                            familyToSkip = dataLib.getKeyValueFamily(keyValue);
                            break;
                        case NEXT_COL:
                            qualifierToSkip = dataLib.getKeyValueQualifier(keyValue);
                            familyToSkip = dataLib.getKeyValueFamily(keyValue);
                            break;
                    }
                }
            }
        }
        return dataLib.newResult(dataLib.getResultKey(result), filteredCells);
    }

    private void ensureTransactionActive(TransactionId transactionId) throws IOException {
        TransactionStruct transaction = transactionStore.getTransactionStatus(transactionId);
        if (!transaction.status.equals(TransactionStatus.ACTIVE)) {
            throw new DoNotRetryIOException("transaction is not ACTIVE");
        }
    }

    private void ensureTransactionAllowsWrites(TransactionId transactionId) throws IOException {
        TransactionStruct transaction = transactionStore.getTransactionStatus(transactionId);
        if (!transaction.allowWrites) {
            throw new DoNotRetryIOException("transaction is read only");
        }
    }

    public boolean shouldKeep(Object keyValue, TransactionId transactionId) throws IOException {
        final long snapshotTimestamp = transactionId.getId();
        final long keyValueTimestamp = dataLib.getKeyValueTimestamp(keyValue);
        final TransactionStruct transaction = transactionStore.getTransactionStatus(keyValueTimestamp);
        switch (transaction.status) {
            case ACTIVE:
                return snapshotTimestamp == keyValueTimestamp;
            case ERROR:
            case ABORT:
                return false;
            case COMMITTING:
                //TODO: needs special handling
                return false;
            case COMMITED:
                return snapshotTimestamp >= transaction.commitTimestamp;
        }
        throw new RuntimeException("unknown transaction status");
    }

    @Override
    public boolean isFilterNeeded(Object operation) {
        Boolean result = dataStore.getSiNeededAttribute(operation);
        if (result == null) {
            return false;
        }
        return result;
    }

    @Override
    public FilterState newFilterState(STable table, TransactionId transactionId) throws IOException {
        ensureTransactionActive(transactionId);
        return new SiFilterState(table, transactionId);
    }

    @Override
    public Filter.ReturnCode filterKeyValue(FilterState filterState, Object keyValue) throws IOException {
        SiFilterState siFilterState = (SiFilterState) filterState;
        Object rowKey = dataLib.getKeyValueRow(keyValue);
        if (siFilterState.currentRowKey == null || !dataLib.valuesEqual(siFilterState.currentRowKey, rowKey)) {
            siFilterState.currentRowKey = rowKey;
            siFilterState.committedTransactions = new HashMap<Long, Long>();
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

    private boolean filterKeepDataValue(Object keyValue, SiFilterState siFilterState) throws IOException {
        long dataTimestamp = dataLib.getKeyValueTimestamp(keyValue);
        Long commitTimestamp = siFilterState.committedTransactions.get(dataTimestamp);
        if (commitTimestamp == null) {
            // debugging code
            final TransactionStruct transactionStatus = transactionStore.getTransactionStatus(dataTimestamp);
            commitTimestamp = transactionStatus.commitTimestamp;
            // If the transaction was committed it should have been included in the committedTransactions map
            assert (commitTimestamp == null);
        }
        if (isCommittedBeforeThisTransaction(siFilterState, commitTimestamp)
                || isThisTransactionsData(siFilterState, dataTimestamp, commitTimestamp)) {
            return true;
        }
        return false;
    }

    private boolean isCommittedBeforeThisTransaction(SiFilterState siFilterState, Long commitTimestamp) {
        return (commitTimestamp != null && commitTimestamp < siFilterState.transactionId.getId());
    }

    private boolean isThisTransactionsData(SiFilterState siFilterState, long dataTimestamp, Long commitTimestamp) {
        return (commitTimestamp == null && dataTimestamp == siFilterState.transactionId.getId());
    }

    private void filterProcessCommitTimestamp(Object keyValue, SiFilterState siFilterState) throws IOException {
        long beginTimestamp = dataLib.getKeyValueTimestamp(keyValue);
        Object commitTimestampValue = dataLib.getKeyValueValue(keyValue);
        Long commitTimestamp = null;
        if (dataStore.isSiNull(commitTimestampValue)) {
            commitTimestamp = filterHandleUnknownTransactionStatus(siFilterState, keyValue, beginTimestamp, commitTimestamp);
        } else {
            commitTimestamp = (Long) dataLib.decode(commitTimestampValue, Long.class);
        }
        if (commitTimestamp != null) {
            siFilterState.committedTransactions.put(beginTimestamp, commitTimestamp);
        }
    }

    private Long filterHandleUnknownTransactionStatus(SiFilterState siFilterState, Object keyValue,
                                                      long beginTimestamp, Long commitTimestamp) throws IOException {
        TransactionStruct transactionStruct = transactionStore.getTransactionStatus(beginTimestamp);

        switch (transactionStruct.status) {
            case ACTIVE:
                break;
            case ERROR:
            case ABORT:
                cleanupErrors();
                break;
            case COMMITTING:
                //TODO: needs special handling
                break;
            case COMMITED:
                rollForward(siFilterState, keyValue, transactionStruct);
                commitTimestamp = transactionStruct.commitTimestamp;
                break;
        }
        return commitTimestamp;
    }

    private void rollForward(SiFilterState siFilterState, Object keyValue, TransactionStruct transactionStruct) {
        dataStore.setCommitTimestamp(siFilterState.table, keyValue, transactionStruct.beginTimestamp, transactionStruct.commitTimestamp);
    }

    private void cleanupErrors() {
        //TODO: implement this
    }
}
