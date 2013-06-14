package com.splicemachine.si.impl;

import com.splicemachine.si.api.Clock;
import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.api.TransactorListener;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableWriter;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.splicemachine.si.impl.TransactionStatus.ACTIVE;
import static com.splicemachine.si.impl.TransactionStatus.COMMITTED;
import static com.splicemachine.si.impl.TransactionStatus.COMMITTING;
import static com.splicemachine.si.impl.TransactionStatus.ERROR;
import static com.splicemachine.si.impl.TransactionStatus.ROLLED_BACK;

/**
 * Central point of implementation of the "snapshot isolation" MVCC algorithm that provides transactions across atomic
 * row updates in the underlying store. This is the core brains of the SI logic.
 */
public class SITransactor<Table, Put, Get, Scan, Mutation, Result, KeyValue, Data, OperationWithAttributes, Delete, Lock>
        implements Transactor<Table, Put, Get, Scan, Mutation, Result, KeyValue, Data> {
    static final Logger LOG = Logger.getLogger(SITransactor.class);

    private final TimestampSource timestampSource;
    private final SDataLib<Data, Result, KeyValue, Put, Delete, Get, Scan, OperationWithAttributes, Lock> dataLib;
    private final STableWriter<Table, Put, Delete, Data, Lock> dataWriter;
    private final DataStore<Data, Result, KeyValue, Put, Delete, Get, Scan, OperationWithAttributes, Table, Lock> dataStore;
    private final TransactionStore transactionStore;
    private final Clock clock;
    private final int transactionTimeoutMS;

    private final TransactorListener listener;

    private final TransactionSource transactionSource;

    public SITransactor(TimestampSource timestampSource, SDataLib dataLib, STableWriter dataWriter, DataStore dataStore,
                        final TransactionStore transactionStore, Clock clock, int transactionTimeoutMS,
                        TransactorListener listener) {
        transactionSource = new TransactionSource() {
            @Override
            public Transaction getTransaction(long timestamp) throws IOException {
                return transactionStore.getTransaction(timestamp);
            }
        };

        this.timestampSource = timestampSource;
        this.dataLib = dataLib;
        this.dataWriter = dataWriter;
        this.dataStore = dataStore;
        this.transactionStore = transactionStore;
        this.clock = clock;
        this.transactionTimeoutMS = transactionTimeoutMS;
        this.listener = listener;
    }

    // Transaction control

    @Override
    public TransactionId beginTransaction() throws IOException {
        return beginTransaction(true);
    }

    @Override
    public TransactionId beginTransaction(boolean allowWrites) throws IOException {
        return beginTransaction(allowWrites, false, false);
    }

    @Override
    public TransactionId beginTransaction(boolean allowWrites, boolean readUncommitted, boolean readCommitted)
            throws IOException {
        return beginChildTransaction(Transaction.getRootTransaction().getTransactionId(), true, allowWrites, readUncommitted,
                readCommitted);
    }

    @Override
    public TransactionId beginChildTransaction(TransactionId parent, boolean allowWrites) throws IOException {
        return beginChildTransaction(parent, true, allowWrites);
    }

    @Override
    public TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites) throws IOException {
        return beginChildTransaction(parent, dependent, allowWrites, null, null);
    }

    @Override
    public TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites, Boolean readUncommitted,
                                               Boolean readCommitted) throws IOException {
        if (allowWrites || readCommitted != null || readUncommitted != null) {
            final TransactionParams params = new TransactionParams(parent, dependent, allowWrites, readUncommitted, readCommitted);
            final long timestamp = assignTransactionId();
            final long beginTimestamp = getBeginTimestamp(timestamp, params.parent.getId());
            transactionStore.recordNewTransaction(timestamp, params, ACTIVE, beginTimestamp, 0L);
            transactionStore.addChildToTransaction(params.parent.getId(), timestamp);
            listener.beginTransaction(!parent.isRootTransaction());
            return new TransactionId(timestamp);
        } else {
            return createLightweightChildTransaction(parent.getId());
        }
    }

    private long assignTransactionId() throws IOException {
        return timestampSource.nextTimestamp();
    }

    /**
     * Generate the next sequential timestamp / transaction ID.
     *
     * @return the new transaction ID.
     */
    private long getBeginTimestamp(long transactionId, long parentId) throws IOException {
        if (parentId == Transaction.ROOT_ID) {
            return transactionId;
        } else {
            return transactionStore.getTimestamp(parentId);
        }
    }

    private long getCommitTimestamp(long parentId) throws IOException {
        if (parentId == Transaction.ROOT_ID) {
            return timestampSource.nextTimestamp();
        } else {
            return transactionStore.getTimestamp(parentId);
        }
    }

    private Long getGlobalCommitTimestamp(Transaction transaction) throws IOException {
        if (transaction.needsGlobalCommitTimestamp()) {
            return timestampSource.nextTimestamp();
        } else {
            return null;
        }
    }

    /**
     * Create a non-resource intensive child. This avoids hitting the transaction table. The same transaction ID is
     * given to many callers, and calls to commit, rollback, etc are ignored.
     */
    private TransactionId createLightweightChildTransaction(long parent) {
        return new TransactionId(parent, true);
    }

    @Override
    public void keepAlive(TransactionId transactionId) throws IOException {
        if (!isIndependentReadOnly(transactionId)) {
            transactionStore.recordKeepAlive(transactionId.getId());
        }
    }

    @Override
    public void commit(TransactionId transactionId) throws IOException {
        if (!isIndependentReadOnly(transactionId)) {
            final Transaction transaction = transactionStore.getTransaction(transactionId.getId());
            ensureTransactionActive(transaction);
            performCommit(transaction);
            listener.commitTransaction();
        }
    }

    /**
     * Update the transaction table to show this transaction is committed.
     */
    private void performCommit(Transaction transaction) throws IOException {
        final long transactionId = transaction.getLongTransactionId();
        if (!transactionStore.recordTransactionStatusChange(transactionId, ACTIVE, COMMITTING)) {
            throw new IOException("committing failed");
        }
        Tracer.traceCommitting(transaction.getLongTransactionId());
        final Long globalCommitTimestamp = getGlobalCommitTimestamp(transaction);
        final long commitTimestamp = getCommitTimestamp(transaction.getParent().getLongTransactionId());
        if (!transactionStore.recordTransactionEnd(transactionId, commitTimestamp, globalCommitTimestamp, COMMITTING, COMMITTED)) {
            throw new DoNotRetryIOException("commit failed");
        }
    }

    @Override
    public void rollback(TransactionId transactionId) throws IOException {
        if (!isIndependentReadOnly(transactionId)) {
            listener.rollbackTransaction();
            rollbackDirect(transactionId.getId());
        }
    }

    private void rollbackDirect(long transactionId) throws IOException {
        Transaction transaction = transactionStore.getTransaction(transactionId);
        // currently the application above us tries to rollback already committed transactions.
        // This is poor form, but if it happens just silently ignore it.
        if (transaction.getStatus().equals(ACTIVE)) {
            if (!transactionStore.recordTransactionStatusChange(transactionId, ACTIVE, ROLLED_BACK)) {
                throw new IOException("rollback failed");
            }
        }
    }

    @Override
    public void fail(TransactionId transactionId) throws IOException {
        if (!isIndependentReadOnly(transactionId)) {
            listener.failTransaction();
            failDirect(transactionId.getId());
        }
    }

    private void failDirect(long transactionId) throws IOException {
        transactionStore.recordTransactionStatusChange(transactionId, ACTIVE, ERROR);
    }

    private boolean isIndependentReadOnly(TransactionId transactionId) {
        return transactionId.independentReadOnly;
    }

    // Transaction ID manipulation

    @Override
    public TransactionId transactionIdFromString(String transactionId) {
        return new TransactionId(transactionId);
    }

    @Override
    public TransactionId transactionIdFromGet(Get operation) {
        return dataStore.getTransactionIdFromOperation((OperationWithAttributes) operation);
    }

    @Override
    public TransactionId transactionIdFromScan(Scan operation) {
        return dataStore.getTransactionIdFromOperation((OperationWithAttributes) operation);
    }

    @Override
    public TransactionId transactionIdFromPut(Put operation) {
        return dataStore.getTransactionIdFromOperation((OperationWithAttributes) operation);
    }

    // Operation initialization. These are expected to be called "client-side" when operations are created.

    @Override
    public void initializeGet(String transactionId, Get get) throws IOException {
        initializeOperation(transactionId, (OperationWithAttributes) get);
    }

    @Override
    public void initializeGet(String transactionId, Get get, boolean includeSIColumn) throws IOException {
        initializeOperation(transactionId, (OperationWithAttributes) get, includeSIColumn, false);
    }

    @Override
    public void initializeScan(String transactionId, Scan scan) {
        initializeOperation(transactionId, (OperationWithAttributes) scan);
    }

    @Override
    public void initializeScan(String transactionId, Scan scan, boolean includeSIColumn, boolean includeUncommittedAsOfStart) {
        initializeOperation(transactionId, (OperationWithAttributes) scan, includeSIColumn, includeUncommittedAsOfStart);
    }

    @Override
    public void initializePut(String transactionId, Put put) {
        initializeOperation(transactionId, (OperationWithAttributes) put);
        dataStore.addPlaceHolderColumnToEmptyPut(put);
    }

    private void initializeOperation(String transactionId, OperationWithAttributes operation) {
        initializeOperation(transactionId, operation, false, false);
    }

    private void initializeOperation(String transactionId, OperationWithAttributes operation, boolean includeSIColumn,
                                     boolean includeUncommittedAsOfStart) {
        flagForSITreatment(transactionIdFromString(transactionId).getId(), includeSIColumn,
                includeUncommittedAsOfStart, operation);
    }

    @Override
    public Put createDeletePut(TransactionId transactionId, Data rowKey) {
        return createDeletePutDirect(transactionId.getId(), rowKey);
    }

    /**
     * Create a "put" operation that will effectively delete a given row.
     */
    private Put createDeletePutDirect(long transactionId, Data rowKey) {
        final Put deletePut = (Put) dataLib.newPut(rowKey);
        flagForSITreatment(transactionId, false, false, (OperationWithAttributes) deletePut);
        dataStore.setTombstoneOnPut(deletePut, transactionId);
        dataStore.setDeletePutAttribute(deletePut);
        return deletePut;
    }

    @Override
    public boolean isDeletePut(Mutation put) {
        final Boolean deleteAttribute = dataStore.getDeletePutAttribute((OperationWithAttributes) put);
        return (deleteAttribute != null && deleteAttribute);
    }

    /**
     * Set an attribute on the operation that identifies it as needing "snapshot isolation" treatment. This is so that
     * later when the operation comes through for processing we will know how to handle it.
     */
    private void flagForSITreatment(long transactionId, boolean includeSIColumn,
                                    boolean includeUncommittedAsOfStart, OperationWithAttributes operation) {
        dataStore.setSINeededAttribute(operation, includeSIColumn);
        if (includeUncommittedAsOfStart) {
            dataStore.setIncludeUncommittedAsOfStart(operation);
        }
        dataStore.setTransactionId(transactionId, operation);
    }

    // Operation pre-processing. These are to be called "server-side" when we are about to process an operation.


    @Override
    public void preProcessGet(Get readOperation) throws IOException {
        dataLib.setGetTimeRange(readOperation, 0, Long.MAX_VALUE);
        dataLib.setGetMaxVersions(readOperation);
        if (dataStore.isIncludeSIColumn((OperationWithAttributes) readOperation)) {
            dataStore.addSIFamilyToGet(readOperation);
        } else {
            dataStore.addSIFamilyToGetIfNeeded(readOperation);
        }
    }

    @Override
    public void preProcessScan(Scan readOperation) throws IOException {
        dataLib.setScanTimeRange(readOperation, 0, Long.MAX_VALUE);
        dataLib.setScanMaxVersions(readOperation);
        if (dataStore.isIncludeSIColumn((OperationWithAttributes) readOperation)) {
            dataStore.addSIFamilyToScan(readOperation);
        } else {
            dataStore.addSIFamilyToScanIfNeeded(readOperation);
        }
    }

    // Process update operations

    @Override
    public boolean processPut(Table table, RollForwardQueue<Data> rollForwardQueue, Put put) throws IOException {
        if (isFlaggedForSITreatment((OperationWithAttributes) put)) {
            processPutDirect(table, rollForwardQueue, (Put) put);
            return true;
        } else {
            return false;
        }
    }

    private void processPutDirect(Table table, RollForwardQueue<Data> rollForwardQueue, Put put) throws IOException {
        final TransactionId transactionId = dataStore.getTransactionIdFromOperation((OperationWithAttributes) put);
        final ImmutableTransaction transaction = transactionStore.getImmutableTransaction(transactionId);
        ensureTransactionAllowsWrites(transaction);
        performPut(table, rollForwardQueue, put, transaction);
    }


    private void performPut(Table table, RollForwardQueue<Data> rollForwardQueue, Put put, ImmutableTransaction transaction)
            throws IOException {
        final Data rowKey = dataLib.getPutKey(put);
        final Lock lock = dataWriter.lockRow(table, rowKey);
        Set<Long> dataTransactionsToRollForward;
        // This is the critical section that runs while the row is locked.
        try {
            final Object[] conflictResults = ensureNoWriteConflict(transaction, table, rowKey);
            dataTransactionsToRollForward = (Set<Long>) conflictResults[0];
            Set<Long> conflictingChildren = (Set<Long>) conflictResults[1];
            final Put newPut = createUltimatePut(transaction, lock, put, table);
            dataStore.suppressIndexing((OperationWithAttributes) newPut);
            dataWriter.write(table, newPut, lock);
            resolveChildConflicts(table, newPut, lock, conflictingChildren);
        } finally {
            dataWriter.unLockRow(table, lock);
        }
        dataStore.recordRollForward(rollForwardQueue, transaction, rowKey);
        for (Long id : dataTransactionsToRollForward) {
            dataStore.recordRollForward(rollForwardQueue, id, rowKey);
        }
    }

    private void resolveChildConflicts(Table table, Put put, Lock lock, Set<Long> conflictingChildren) throws IOException {
        if (!conflictingChildren.isEmpty()) {
            Delete delete = dataStore.copyPutToDelete(put, conflictingChildren);
            dataStore.suppressIndexing((OperationWithAttributes) delete);
            dataWriter.delete(table, delete, lock);
        }
    }

    /**
     * While we hold the lock on the row, check to make sure that no transactions have updated the row since the
     * updating transaction started.
     */
    private Object[] ensureNoWriteConflict(ImmutableTransaction updateTransaction, Table table, Data rowKey)
            throws IOException {
        final List<KeyValue> dataCommitKeyValues = dataStore.getCommitTimestamp(table, rowKey);
        if (dataCommitKeyValues != null) {
            return checkCommitTimestampsForConflicts(updateTransaction, dataCommitKeyValues);
        }
        return new Object[]{Collections.EMPTY_SET, Collections.EMPTY_SET};
    }

    /**
     * Look at all of the values in the "commitTimestamp" column to see if there are write collisions.
     */
    private Object[] checkCommitTimestampsForConflicts(ImmutableTransaction updateTransaction, List<KeyValue> dataCommitKeyValues)
            throws IOException {
        Set<Long> toRollForward = new HashSet<Long>();
        Set<Long> childConflicts = new HashSet<Long>();
        for (KeyValue dataCommitKeyValue : dataCommitKeyValues) {
            checkCommitTimestampForConflict(updateTransaction, toRollForward, childConflicts, dataCommitKeyValue);
        }
        return new Object[]{toRollForward, childConflicts};
    }

    private void checkCommitTimestampForConflict(ImmutableTransaction updateTransaction, Set<Long> toRollForward, Set<Long> childConflicts, KeyValue dataCommitKeyValue) throws IOException {
        final long dataTransactionId = dataLib.getKeyValueTimestamp(dataCommitKeyValue);
        if (!updateTransaction.sameTransaction(dataTransactionId)) {
            final Data commitTimestampValue = dataLib.getKeyValueValue(dataCommitKeyValue);
            if (dataStore.isSINull(commitTimestampValue)) {
                Transaction dataTransaction = transactionStore.getTransaction(dataTransactionId);
                dataTransaction = checkTransactionTimeout(dataTransaction);
                final ConflictType conflictType = checkTransactionConflict(updateTransaction, dataTransaction);
                if (conflictType.equals(ConflictType.CHILD)) {
                    childConflicts.add(dataTransactionId);
                }
                toRollForward.add(dataTransactionId);
            } else if (dataStore.isSIFail(commitTimestampValue)) {
            } else {
                final long dataCommitTimestamp = (Long) dataLib.decode(commitTimestampValue, Long.class);
                if (dataCommitTimestamp > updateTransaction.getBeginTimestamp()) {
                    failOnWriteConflict(updateTransaction);
                }
            }
        }
    }

    /**
     * Look at the last keepAlive timestamp on the transaction, if it is too long in the past then "fail" the
     * transaction. Returns a possibly updated transaction that reflects the call to "fail".
     */
    private Transaction checkTransactionTimeout(Transaction dataTransaction) throws IOException {
        if ((clock.getTime() - dataTransaction.keepAlive) > transactionTimeoutMS) {
            fail(dataTransaction.getTransactionId());
            return transactionStore.getTransaction(dataTransaction.getLongTransactionId());
        } else {
            return dataTransaction;
        }
    }

    /**
     * Determine if the dataTransaction conflicts with the updateTransaction.
     */
    private ConflictType checkTransactionConflict(ImmutableTransaction updateTransaction, Transaction dataTransaction)
            throws IOException {
        if (!updateTransaction.sameTransaction(dataTransaction)) {
            Transaction t1 = transactionStore.getTransaction(updateTransaction.getLongTransactionId());
            final ConflictType conflict = t1.isConflict(dataTransaction, transactionSource);
            switch (conflict) {
                case NONE:
                    break;
                case SIBLING:
                    failOnWriteConflict(updateTransaction);
                    break;
            }
            return conflict;
        }
        return ConflictType.NONE;
    }

    /**
     * A write conflict was discovered, throw an exception and kill the offending transaction.
     */
    private void failOnWriteConflict(ImmutableTransaction transaction) throws IOException {
        fail(transaction.getTransactionId());
        throw new WriteConflict("write/write conflict");
    }

    /**
     * Create a new operation, with the lock, that has all of the keyValues from the original operation.
     * This will also set the timestamp of the data being updated to reflect the transaction doing the update.
     */
    Put createUltimatePut(ImmutableTransaction transaction, Lock lock, Put put, Table table) throws IOException {
        final Data rowKey = dataLib.getPutKey(put);
        final Put newPut = dataLib.newPut(rowKey, lock);
        final long timestamp = transaction.getLongTransactionId();
        dataStore.copyPutKeyValues(put, true, newPut, timestamp);
        dataStore.addTransactionIdToPut(newPut, timestamp);
        if (isDeletePut((Mutation) put)) {
            dataStore.setTombstonesOnColumns(table, timestamp, newPut);
        }
        return newPut;
    }

    // Process read operations

    @Override
    public boolean isFilterNeededGet(Get get) {
        return isFlaggedForSITreatment((OperationWithAttributes) get)
                && !dataStore.isSuppressIndexing((OperationWithAttributes) get);
    }

    @Override
    public boolean isFilterNeededScan(Scan scan) {
        return isFlaggedForSITreatment((OperationWithAttributes) scan)
                && !dataStore.isSuppressIndexing((OperationWithAttributes) scan);
    }

    @Override
    public boolean isGetIncludeSIColumn(Get get) {
        return dataStore.isIncludeSIColumn((OperationWithAttributes) get);
    }

    @Override
    public boolean isScanIncludeSIColumn(Scan read) {
        return dataStore.isIncludeSIColumn((OperationWithAttributes) read);
    }

    @Override
    public boolean isScanIncludeUncommittedAsOfStart(Scan scan) {
        return dataStore.isScanIncludeUncommittedAsOfStart((OperationWithAttributes) scan);
    }

    @Override
    public FilterState newFilterState(TransactionId transactionId) throws IOException {
        return newFilterState(null, transactionId, false, false);
    }

    @Override
    public FilterState newFilterState(RollForwardQueue rollForwardQueue, TransactionId transactionId,
                                      boolean includeSIColumn, boolean includeUncommittedAsOfStart)
            throws IOException {
        return new FilterState(dataLib, dataStore, transactionStore, rollForwardQueue, includeSIColumn,
                includeUncommittedAsOfStart, transactionStore.getImmutableTransaction(transactionId));
    }

    @Override
    public Filter.ReturnCode filterKeyValue(FilterState filterState, KeyValue keyValue) throws IOException {
        return filterState.filterKeyValue(keyValue);
    }

    @Override
    public Result filterResult(FilterState filterState, Result result) throws IOException {
        final SDataLib<Data, Result, KeyValue, Put, Delete, Get, Scan, OperationWithAttributes, Lock> dataLib = dataStore.dataLib;
        final List<KeyValue> filteredCells = new ArrayList<KeyValue>();
        final List<KeyValue> keyValues = dataLib.listResult(result);
        if (keyValues != null) {
            Data qualifierToSkip = null;
            Data familyToSkip = null;

            for (KeyValue keyValue : keyValues) {
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
                            break;
                        case NEXT_COL:
                            qualifierToSkip = dataLib.getKeyValueQualifier(keyValue);
                            familyToSkip = dataLib.getKeyValueFamily(keyValue);
                            break;
                    }
                }
            }
        }
        if (filteredCells.isEmpty()) {
            return null;
        } else {
            return dataLib.newResult(dataLib.getResultKey(result), filteredCells);
        }
    }

    // Roll-forward / compaction

    @Override
    public void rollForward(Table table, long transactionId, List<Data> rows) throws IOException {
        final Transaction transaction = transactionStore.getTransaction(transactionId);
        final TransactionStatus effectiveStatus = transaction.getEffectiveStatus();
        if (effectiveStatus.equals(COMMITTED) || effectiveStatus.equals(ROLLED_BACK) || effectiveStatus.equals(ERROR)) {
            for (Data row : rows) {
                try {
                    if (effectiveStatus.equals(COMMITTED)) {
                        dataStore.setCommitTimestamp(table, row, transaction.getLongTransactionId(), transaction.getCommitTimestamp());
                    } else {
                        dataStore.setCommitTimestampToFail(table, row, transaction.getLongTransactionId());
                    }
                    Tracer.traceRowRollForward(row);
                } catch (NotServingRegionException e) {
                    // If the region split and the row is not here, then just skip it
                }
            }
        }
        Tracer.traceTransactionRollForward(transactionId);
    }

    @Override
    public SICompactionState newCompactionState() {
        return new SICompactionState(dataLib, dataStore, transactionStore);
    }

    // Helpers

    /**
     * Is this operation supposed to be handled by "snapshot isolation".
     */
    private boolean isFlaggedForSITreatment(OperationWithAttributes operation) {
        return dataStore.getSINeededAttribute(operation) != null;
    }

    /**
     * Throw an exception if the transaction is not active.
     */
    private void ensureTransactionActive(Transaction transaction) throws IOException {
        if (!transaction.getStatus().equals(ACTIVE)) {
            throw new DoNotRetryIOException("transaction is not ACTIVE: " +
                    transaction.getTransactionId().getTransactionIdString());
        }
    }

    /**
     * Throw an exception if this is a read-only transaction.
     */
    private void ensureTransactionAllowsWrites(ImmutableTransaction transaction) throws IOException {
        if (transaction.isReadOnly()) {
            throw new DoNotRetryIOException("transaction is read only: " + transaction.getTransactionId().getTransactionIdString());
        }
    }

}
