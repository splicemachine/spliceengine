package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.SGet;
import com.splicemachine.si.data.api.SRowLock;
import com.splicemachine.si.data.api.SScan;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.api.ClientTransactor;
import com.splicemachine.si.api.FilterState;
import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.api.Transactor;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
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
    public TransactionId beginTransaction(boolean allowWrites, boolean readUncommitted, boolean readCommitted)
            throws IOException {
        return beginTransactionDirect(null, null, TransactionStatus.ACTIVE, allowWrites, readUncommitted, readCommitted);
    }

    @Override
    public TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites,
                                               Boolean readUncommitted, Boolean readCommitted) throws IOException {
        final TransactionId childTransactionId = beginTransactionDirect(parent, dependent, null, allowWrites, readUncommitted, readCommitted);
        transactionStore.addChildToTransaction(parent, childTransactionId);
        return childTransactionId;
    }

    private TransactionId beginTransactionDirect(TransactionId parent, Boolean dependent, TransactionStatus status,
                                                 boolean allowWrites, Boolean readUncommitted, Boolean readCommitted) throws IOException {
        final SiTransactionId transactionId = new SiTransactionId(timestampSource.nextTimestamp());
        transactionStore.recordNewTransaction(transactionId, parent, dependent, allowWrites,
                readUncommitted, readCommitted, status);
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
    public TransactionId getTransactionIdFromGet(Object get) {
        return dataStore.getTransactionIdFromOperation(get);
    }

    @Override
    public TransactionId getTransactionIdFromScan(Object scan) {
        return dataStore.getTransactionIdFromOperation(scan);
    }

    @Override
    public void commit(TransactionId transactionId) throws IOException {
        final Transaction transaction = ensureTransactionActive(transactionId);
        if (!transaction.isNestedDependent()) {
            List<Long> childrenToCommit = new ArrayList<Long>();
            for (Long childId : transaction.getChildren()) {
                if (transactionStore.getTransactionStatus(childId).isEffectivelyActive()) {
                    childrenToCommit.add(childId);
                }
            }
            transactionStore.recordTransactionStatusChange(transactionId, TransactionStatus.COMMITTING);
            // TODO: need to sort out how to take child transactions through COMMITTING state
            final long endId = timestampSource.nextTimestamp();
            transactionStore.recordTransactionCommit(transactionId, endId, TransactionStatus.COMMITTED);
            for (Long childId : childrenToCommit) {
                commitChild(childId, endId);
            }
        } else {
            // perform "local" commit only within the parent transaction
            transactionStore.recordTransactionStatusChange(transactionId, TransactionStatus.LOCAL_COMMIT);
        }
    }

    private void commitChild(long childId, long endId) throws IOException {
        transactionStore.recordTransactionCommit(new SiTransactionId(childId), endId, TransactionStatus.COMMITTED);
    }

    @Override
    public void rollback(TransactionId transactionId) throws IOException {
        Transaction transaction = transactionStore.getTransactionStatus(transactionId);
        if (transaction.isActive() && !transaction.isLocallyCommitted()) {
            transactionStore.recordTransactionStatusChange(transactionId, TransactionStatus.ROLLED_BACK);
        }
    }

    @Override
    public void fail(TransactionId transactionId) throws IOException {
        ensureTransactionActive(transactionId);
        transactionStore.recordTransactionStatusChange(transactionId, TransactionStatus.ERROR);
    }

    @Override
    public void initializeGet(String transactionId, SGet get) throws IOException {
        dataStore.setSiNeededAttribute(get);
        dataStore.setTransactionId((SiTransactionId) transactionIdFromString(transactionId), get);
    }

    @Override
    public void preProcessGet(SGet get) throws IOException {
        setGetTimeRange(get);
        dataLib.setGetMaxVersions(get);
        dataStore.ensureSiFamilyOnGet(get);

    }

    private void setGetTimeRange(SGet get) {
        dataLib.setGetTimeRange(get, 0, Long.MAX_VALUE);
    }

    @Override
    public void initializeScan(String transactionId, SScan scan) {
        dataStore.setSiNeededAttribute(scan);
        dataStore.setTransactionId((SiTransactionId) transactionIdFromString(transactionId), scan);
    }

    @Override
    public void preProcessScan(SScan scan) {
        dataLib.setScanTimeRange(scan, 0L, Long.MAX_VALUE);
        dataLib.setScanMaxVersions(scan);
        dataStore.ensureSiFamilyOnScan(scan);
    }

    @Override
    public void initializePut(String transactionId, Object put) {
        dataStore.setSiNeededAttribute(put);
        dataStore.setTransactionId((SiTransactionId) transactionIdFromString(transactionId), put);
    }

    @Override
    public Object newDeletePut(TransactionId transactionId, Object rowKey) {
        SiTransactionId siTransactionId = (SiTransactionId) transactionId;
        final Object deletePut = dataLib.newPut(rowKey);
        initializePut(siTransactionId.getTransactionIdString(), deletePut);
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
            final ImmutableTransaction transaction = transactionStore.getImmutableTransaction(transactionId);
            ensureTransactionAllowsWrites(transaction);
            Object rowKey = dataLib.getPutKey(put);
            SRowLock lock = dataWriter.lockRow(table, rowKey);
            try {
                checkForConflict(transaction, table, lock, rowKey);
                Object newPut = dataStore.newLockWithPut(transactionId, put, lock);
                flagPutAsNotNeedingIndexTreatment(newPut);
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

    private void flagPutAsNotNeedingIndexTreatment(Object newPut) {
        dataLib.addAttribute(newPut, "iu", new byte[]{});
    }

    private void checkForConflict(ImmutableTransaction putTransaction, STable table, SRowLock lock, Object rowKey) throws IOException {
        TransactionId transactionId = new SiTransactionId(putTransaction.beginTimestamp);
        List keyValues = dataStore.getCommitTimestamp(table, rowKey);
        if (keyValues != null) {
            int index = 0;
            boolean loop = true;
            while (loop) {
                if (index >= keyValues.size()) {
                    loop = false;
                } else {
                    Object dataKeyValue = keyValues.get(index);
                    long dataTransactionId = dataLib.getKeyValueTimestamp(dataKeyValue);
                    Transaction dataTransaction = transactionStore.getTransactionStatus(dataTransactionId);
                    if (dataTransaction.committedAfter(putTransaction)) {
                        writeWriteConflict(transactionId);
                    } else {
                        if (dataTransaction.isEffectivelyActive() && !dataTransaction.isEffectivelyPartOfTransaction(putTransaction)) {
                            // if the KeyValue was written by the current running transaction then it is not a conflict
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
        throw new WriteConflict("write/write conflict");
    }

    private Transaction ensureTransactionActive(TransactionId transactionId) throws IOException {
        Transaction transaction = transactionStore.getTransactionStatus(transactionId);
        if (!transaction.isEffectivelyActive()) {
            throw new DoNotRetryIOException("transaction is not ACTIVE: " + transactionId.getTransactionIdString());
        }
        return transaction;
    }

    private void ensureTransactionAllowsWrites(ImmutableTransaction transaction) throws IOException {
        if (transaction.isReadOnly()) {
            throw new DoNotRetryIOException("transaction is read only");
        }
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
        final ImmutableTransaction immutableTransaction = transactionStore.getImmutableTransaction(transactionId);
        return new SiFilterState(dataLib, dataStore, transactionStore, table, immutableTransaction);
    }

    @Override
    public Filter.ReturnCode filterKeyValue(FilterState filterState, Object keyValue) throws IOException {
        SiFilterState siFilterState = (SiFilterState) filterState;
        return siFilterState.filterKeyValue(keyValue);
    }

}
