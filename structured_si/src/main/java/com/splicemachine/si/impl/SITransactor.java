package com.splicemachine.si.impl;

import com.splicemachine.si.api.Clock;
import com.splicemachine.si.api.PutToRun;
import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.api.TransactorListener;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.SRowLock;
import com.splicemachine.si.data.api.STableWriter;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
public class SITransactor<Table, OperationWithAttributes, Mutation extends OperationWithAttributes, Put extends Mutation, Get extends OperationWithAttributes,
        Scan extends OperationWithAttributes, Result, KeyValue, Data, Hashable,
        Delete extends OperationWithAttributes, Lock extends SRowLock>
        implements Transactor<Table, Put, Get, Scan, Mutation, Result, KeyValue, Data, Hashable, Lock> {
    static final Logger LOG = Logger.getLogger(SITransactor.class);

    private final TimestampSource timestampSource;
    private final SDataLib<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock> dataLib;
    private final STableWriter<Table, Put, Delete, Data, Lock> dataWriter;
    private final DataStore<Data, Hashable, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Table, Lock> dataStore;
    private final TransactionStore transactionStore;
    private final Clock clock;
    private final int transactionTimeoutMS;
    private final Hasher<Data, Hashable> hasher;

    private final TransactorListener listener;

    private final TransactionSource transactionSource;

    public SITransactor(TimestampSource timestampSource, SDataLib dataLib, STableWriter dataWriter, DataStore dataStore,
                        final TransactionStore transactionStore, Clock clock, int transactionTimeoutMS,
                        Hasher<Data, Hashable> hasher, TransactorListener listener) {
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
        this.hasher = hasher;
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
        return beginChildTransaction(Transaction.rootTransaction.getTransactionId(), true, allowWrites, readUncommitted,
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
            final long beginTimestamp = generateBeginTimestamp(timestamp, params.parent.getId());
            transactionStore.recordNewTransaction(timestamp, params, ACTIVE, beginTimestamp, 0L);
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
    private long generateBeginTimestamp(long transactionId, long parentId) throws IOException {
        if (parentId == Transaction.ROOT_ID) {
            return transactionId;
        } else {
            return transactionStore.generateTimestamp(parentId);
        }
    }

    private long generateCommitTimestamp(long parentId) throws IOException {
        if (parentId == Transaction.ROOT_ID) {
            return timestampSource.nextTimestamp();
        } else {
            return transactionStore.generateTimestamp(parentId);
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
            transactionStore.recordTransactionKeepAlive(transactionId.getId());
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
        final long commitTimestamp = generateCommitTimestamp(transaction.getParent().getLongTransactionId());
        if (!transactionStore.recordTransactionCommit(transactionId, commitTimestamp, globalCommitTimestamp, COMMITTING, COMMITTED)) {
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
        if (transaction.status.isActive()) {
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
        return dataStore.getTransactionIdFromOperation(operation);
    }

    @Override
    public TransactionId transactionIdFromScan(Scan operation) {
        return dataStore.getTransactionIdFromOperation(operation);
    }

    @Override
    public TransactionId transactionIdFromPut(Put operation) {
        return dataStore.getTransactionIdFromOperation(operation);
    }

    // Operation initialization. These are expected to be called "client-side" when operations are created.

    @Override
    public void initializeGet(String transactionId, Get get) throws IOException {
        initializeOperation(transactionId, get, true, false);
    }

    @Override
    public void initializeScan(String transactionId, Scan scan, boolean includeSIColumn, boolean includeUncommittedAsOfStart) {
        initializeOperation(transactionId, scan, includeSIColumn, includeUncommittedAsOfStart);
    }

    @Override
    public void initializePut(String transactionId, Put put) {
        initializeOperation(transactionId, put);
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
        final Put deletePut = dataLib.newPut(rowKey);
        flagForSITreatment(transactionId, false, false, deletePut);
        dataStore.setTombstoneOnPut(deletePut, transactionId);
        dataStore.setDeletePutAttribute(deletePut);
        return deletePut;
    }

    @Override
    public boolean isDeletePut(Mutation mutation) {
        final Boolean deleteAttribute = dataStore.getDeletePutAttribute(mutation);
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
        if (dataStore.isIncludeSIColumn(readOperation)) {
            dataStore.addSIFamilyToGet(readOperation);
        } else {
            dataStore.addSIFamilyToGetIfNeeded(readOperation);
        }
    }

    @Override
    public void preProcessScan(Scan readOperation) throws IOException {
        dataLib.setScanTimeRange(readOperation, 0, Long.MAX_VALUE);
        dataLib.setScanMaxVersions(readOperation);
        if (dataStore.isIncludeSIColumn(readOperation)) {
            dataStore.addSIFamilyToScan(readOperation);
        } else {
            dataStore.addSIFamilyToScanIfNeeded(readOperation);
        }
    }

    // Process update operations

    @Override
    public boolean processPut(Table table, RollForwardQueue<Data, Hashable> rollForwardQueue, Put put) throws IOException {
        return processPutWithSICheck(table, rollForwardQueue, put);
    }

    @Override
    public PutToRun<Mutation> preProcessBatchPut(Table table, RollForwardQueue<Data, Hashable> rollForwardQueue,
                                                 Put put, Map<Hashable, Lock> locks) throws IOException {
        if (isFlaggedForSITreatment(put)) {
            final TransactionId transactionId = dataStore.getTransactionIdFromOperation(put);
            final ImmutableTransaction transaction = transactionStore.getImmutableTransaction(transactionId);
            ensureTransactionAllowsWrites(transaction);
            final Data rowKey = dataLib.getPutKey(put);
            final Set<Long> dataTransactionsToRollForward;

            final Lock lock = obtainLock(locks, table, rowKey);
            // This is the critical section that runs while the row is locked.
            //try {
            final Object[] conflictResults = ensureNoWriteConflict(transaction, table, rowKey);
            dataTransactionsToRollForward = (Set<Long>) conflictResults[0];
            final Set<Long> conflictingChildren = (Set<Long>) conflictResults[1];
            final Put newPut = createUltimatePut(transaction.getLongTransactionId(), lock, put, table, (Boolean) conflictResults[2]);
            dataStore.suppressIndexing(newPut);
            //dataWriter.write(table, newPut, lock);
            //resolveChildConflicts(table, put, lock, conflictingChildren);
            //} finally {
            // dataWriter.unLockRow(table, lock);
            //}
            dataStore.recordRollForward(rollForwardQueue, transaction.getLongTransactionId(), rowKey);
            for (Long transactionIdToRollForward : dataTransactionsToRollForward) {
                dataStore.recordRollForward(rollForwardQueue, transactionIdToRollForward, rowKey);
            }
            return new PutToRun<Mutation>(new Pair<Mutation, Integer>(newPut, lock.toInteger()), conflictingChildren);
        } else {
            return new PutToRun<Mutation>(new Pair<Mutation, Integer>(put, null), Collections.<Long>emptySet());
        }
    }

    private Lock obtainLock(Map<Hashable, Lock> locks, Table table, Data rowKey) throws IOException {
        final Hashable hashableRowKey = hasher.toHashable(rowKey);
        Lock lock = locks.get(hashableRowKey);
        if (lock == null) {
            lock = dataWriter.lockRow(table, rowKey);
            locks.put(hashableRowKey, lock);
        }
        return lock;
    }

    @Override
    public void postProcessBatchPut(Table table, Put put, Lock lock, Set<Long> conflictingChildren) throws IOException {
        if (isFlaggedForSITreatment(put)) {
            try {
                resolveChildConflicts(table, put, lock, conflictingChildren);
            } finally {
                dataWriter.unLockRow(table, lock);
            }
        }
    }

    private boolean processPutWithSICheck(Table table, RollForwardQueue<Data, Hashable> rollForwardQueue, Put put) throws IOException {
        if (isFlaggedForSITreatment(put)) {
            processPutWithWritableCheck(table, rollForwardQueue, put);
            return true;
        } else {
            return false;
        }
    }

    private void processPutWithWritableCheck(Table table, RollForwardQueue<Data, Hashable> rollForwardQueue, Put put) throws IOException {
        final TransactionId transactionId = dataStore.getTransactionIdFromOperation(put);
        final ImmutableTransaction transaction = transactionStore.getImmutableTransaction(transactionId);
        ensureTransactionAllowsWrites(transaction);
        processPutWithConflictCheckAndRollforward(table, rollForwardQueue, put, transaction);
    }


    private void processPutWithConflictCheckAndRollforward(Table table, RollForwardQueue<Data, Hashable> rollForwardQueue, Put put, ImmutableTransaction transaction)
            throws IOException {
        final long transactionId = transaction.getLongTransactionId();
        final Data rowKey = dataLib.getPutKey(put);
        final Set<Long> dataTransactionsToRollForward;

        final Lock lock = dataWriter.lockRow(table, rowKey);
        // This is the critical section that runs while the row is locked.
        try {
            final Object[] conflictResults = ensureNoWriteConflict(transaction, table, rowKey);
            dataTransactionsToRollForward = (Set<Long>) conflictResults[0];
            final Set<Long> conflictingChildren = (Set<Long>) conflictResults[1];
            processPutDirect(table, put, transactionId, lock, (Boolean) conflictResults[2]);
            resolveChildConflicts(table, put, lock, conflictingChildren);
        } finally {
            dataWriter.unLockRow(table, lock);
        }
        dataStore.recordRollForward(rollForwardQueue, transactionId, rowKey);
        for (Long transactionIdToRollForward : dataTransactionsToRollForward) {
            dataStore.recordRollForward(rollForwardQueue, transactionIdToRollForward, rowKey);
        }
    }

    private void processPutDirect(Table table, Put put, long transactionId, Lock lock, boolean hasTombstone) throws IOException {
        final Put newPut = createUltimatePut(transactionId, lock, put, table, hasTombstone);
        dataStore.suppressIndexing(newPut);
        dataWriter.write(table, newPut, lock);
    }

    private void resolveChildConflicts(Table table, Put put, Lock lock, Set<Long> conflictingChildren) throws IOException {
        if (!conflictingChildren.isEmpty()) {
            Delete delete = dataStore.copyPutToDelete(put, conflictingChildren);
            dataStore.suppressIndexing(delete);
            dataWriter.delete(table, delete, lock);
        }
    }

    /**
     * While we hold the lock on the row, check to make sure that no transactions have updated the row since the
     * updating transaction started.
     */
    private Object[] ensureNoWriteConflict(ImmutableTransaction updateTransaction, Table table, Data rowKey)
            throws IOException {
        final List<KeyValue>[] values = dataStore.getCommitTimestampsAndTombstones(table, rowKey);
        final Object[] timestampConflicts = checkTimestampsHandleNull(updateTransaction, values[1]);
        final List<KeyValue> tombstoneValues = values[0];
        boolean hasTombstone = hasCurrentTransactionTombstone(updateTransaction.getLongTransactionId(), tombstoneValues);
        return new Object[]{timestampConflicts[0], timestampConflicts[1], hasTombstone};
    }

    private boolean hasCurrentTransactionTombstone(long transactionId, List<KeyValue> tombstoneValues) {
        if (tombstoneValues != null) {
            for (KeyValue tombstone : tombstoneValues) {
                if (dataLib.getKeyValueTimestamp(tombstone) == transactionId) {
                    return !dataStore.isAntiTombstone(tombstone);
                }
            }
        }
        return false;
    }

    private Object[] checkTimestampsHandleNull(ImmutableTransaction updateTransaction, List<KeyValue> dataCommitKeyValues) throws IOException {
        if (dataCommitKeyValues == null) {
            return new Object[]{Collections.EMPTY_SET, Collections.EMPTY_SET};
        } else {
            return checkCommitTimestampsForConflicts(updateTransaction, dataCommitKeyValues);
        }
    }

    /**
     * Look at all of the values in the "commitTimestamp" column to see if there are write collisions.
     */
    private Object[] checkCommitTimestampsForConflicts(ImmutableTransaction updateTransaction, List<KeyValue> dataCommitKeyValues)
            throws IOException {
        final Set<Long> toRollForward = new HashSet<Long>();
        final Set<Long> childConflicts = new HashSet<Long>();
        for (KeyValue dataCommitKeyValue : dataCommitKeyValues) {
            checkCommitTimestampForConflict(updateTransaction, toRollForward, childConflicts, dataCommitKeyValue);
        }
        return new Object[]{toRollForward, childConflicts};
    }

    private void checkCommitTimestampForConflict(ImmutableTransaction updateTransaction, Set<Long> toRollForward,
                                                 Set<Long> childConflicts, KeyValue dataCommitKeyValue)
            throws IOException {
        final long dataTransactionId = dataLib.getKeyValueTimestamp(dataCommitKeyValue);
        if (!updateTransaction.sameTransaction(dataTransactionId)) {
            final Data commitTimestampValue = dataLib.getKeyValueValue(dataCommitKeyValue);
            if (dataStore.isSINull(commitTimestampValue)) {
                // Unknown transaction status
                final Transaction dataTransaction = transactionStore.getTransaction(dataTransactionId);
                if (dataTransaction.getEffectiveStatus().isFinished()) {
                    // Transaction is now in a final state so asynchronously update the data row with the status
                    toRollForward.add(dataTransactionId);
                }
                final ConflictType conflictType = checkTransactionConflict(updateTransaction, dataTransaction);
                switch (conflictType) {
                    case CHILD:
                        childConflicts.add(dataTransactionId);
                        break;
                    case SIBLING:
                        if (doubleCheckConflict(updateTransaction, dataTransaction)) {
                            throw failOnWriteConflict(updateTransaction);
                        }
                        break;
                }
            } else if (dataStore.isSIFail(commitTimestampValue)) {
                // Can't conflict with failed transaction.
            } else {
                // Committed transaction
                final long dataCommitTimestamp = (Long) dataLib.decode(commitTimestampValue, Long.class);
                if (dataCommitTimestamp > updateTransaction.getEffectiveBeginTimestamp()) {
                    throw failOnWriteConflict(updateTransaction);
                }
            }
        }
    }

    /**
     * @param updateTransaction
     * @param dataTransaction
     * @return true if there is a conflict
     * @throws IOException
     */
    private boolean doubleCheckConflict(ImmutableTransaction updateTransaction, Transaction dataTransaction) throws IOException {
        // If the transaction has timed out then it might be possible to make it fail and proceed without conflict.
        if (checkTransactionTimeout(dataTransaction)) {
            // Double check for conflict if there was a transaction timeout
            final Transaction dataTransaction2 = transactionStore.getTransaction(dataTransaction.getLongTransactionId());
            final ConflictType conflictType2 = checkTransactionConflict(updateTransaction, dataTransaction2);
            if (conflictType2.equals(ConflictType.NONE)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Look at the last keepAlive timestamp on the transaction, if it is too long in the past then "fail" the
     * transaction. Returns true if a timeout was generated.
     */
    private boolean checkTransactionTimeout(Transaction dataTransaction) throws IOException {
        if (!dataTransaction.isRootTransaction()
                && dataTransaction.getEffectiveStatus().isActive()
                && ((clock.getTime() - dataTransaction.keepAlive) > transactionTimeoutMS)) {
            fail(dataTransaction.getTransactionId());
            checkTransactionTimeout(dataTransaction.getParent());
            return true;
        } else if (dataTransaction.isRootTransaction()) {
            return false;
        } else {
            return checkTransactionTimeout(dataTransaction.getParent());
        }
    }

    /**
     * Determine if the dataTransaction conflicts with the updateTransaction.
     */
    private ConflictType checkTransactionConflict(ImmutableTransaction updateTransaction, Transaction dataTransaction)
            throws IOException {
        if (updateTransaction.sameTransaction(dataTransaction)) {
            return ConflictType.NONE;
        } else {
            return updateTransaction.isInConflictWith(dataTransaction, transactionSource);
        }
    }

    /**
     * A write conflict was discovered, throw an exception and kill the offending transaction.
     */
    private WriteConflict failOnWriteConflict(ImmutableTransaction transaction) throws IOException {
        fail(transaction.getTransactionId());
        return new WriteConflict("write/write conflict");
    }

    /**
     * Create a new operation, with the lock, that has all of the keyValues from the original operation.
     * This will also set the timestamp of the data being updated to reflect the transaction doing the update.
     */
    Put createUltimatePut(long transactionId, Lock lock, Put put, Table table, boolean hasTombstone) throws IOException {
        final Data rowKey = dataLib.getPutKey(put);
        final Put newPut = dataLib.newPut(rowKey, lock);
        dataStore.copyPutKeyValues(put, newPut, transactionId);
        dataStore.addTransactionIdToPutKeyValues(newPut, transactionId);
        if (isDeletePut(put)) {
            dataStore.setTombstonesOnColumns(table, transactionId, newPut);
        } else if (hasTombstone) {
            dataStore.setAntiTombstoneOnPut(newPut, transactionId);
        }
        return newPut;
    }

    // Process read operations

    @Override
    public boolean isFilterNeededGet(Get get) {
        return isFlaggedForSITreatment(get)
                && !dataStore.isSuppressIndexing(get);
    }

    @Override
    public boolean isFilterNeededScan(Scan scan) {
        return isFlaggedForSITreatment(scan)
                && !dataStore.isSuppressIndexing(scan);
    }

    @Override
    public boolean isGetIncludeSIColumn(Get get) {
        return dataStore.isIncludeSIColumn(get);
    }

    @Override
    public boolean isScanIncludeSIColumn(Scan read) {
        return dataStore.isIncludeSIColumn(read);
    }

    @Override
    public boolean isScanIncludeUncommittedAsOfStart(Scan scan) {
        return dataStore.isScanIncludeUncommittedAsOfStart(scan);
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
        final SDataLib<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock> dataLib = dataStore.dataLib;
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
        if (transaction.getEffectiveStatus().isFinished()) {
            for (Data row : rows) {
                try {
                    if (transaction.getEffectiveStatus().isCommitted()) {
                        dataStore.setCommitTimestamp(table, row, transaction.getLongTransactionId(), transaction.getEffectiveCommitTimestamp());
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

    @Override
    public void compact(SICompactionState compactionState, List<KeyValue> rawList, List<KeyValue> results) throws IOException {
        compactionState.mutate(rawList, results);
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
        if (transaction.status.isFinished()) {
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
