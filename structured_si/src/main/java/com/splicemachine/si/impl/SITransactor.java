package com.splicemachine.si.impl;

import com.splicemachine.si.api.Clock;
import com.splicemachine.si.api.FilterState;
import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.SGet;
import com.splicemachine.si.data.api.SRead;
import com.splicemachine.si.data.api.SRowLock;
import com.splicemachine.si.data.api.SScan;
import com.splicemachine.si.data.api.STable;
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
public class SITransactor<PutOp, GetOp extends SGet, ScanOp extends SScan, MutationOp, ResultType>
        implements Transactor<PutOp, GetOp, ScanOp, MutationOp, ResultType> {
    static final Logger LOG = Logger.getLogger(SITransactor.class);

    private final TimestampSource timestampSource;
    private final SDataLib dataLib;
    private final STableWriter dataWriter;
    private final DataStore dataStore;
    private final TransactionStore transactionStore;
    private final Clock clock;
    private final int transactionTimeoutMS;
    private Transaction transaction;

    public SITransactor(TimestampSource timestampSource, SDataLib dataLib, STableWriter dataWriter, DataStore dataStore,
                        TransactionStore transactionStore, Clock clock, int transactionTimeoutMS) {
        this.timestampSource = timestampSource;
        this.dataLib = dataLib;
        this.dataWriter = dataWriter;
        this.dataStore = dataStore;
        this.transactionStore = transactionStore;
        this.clock = clock;
        this.transactionTimeoutMS = transactionTimeoutMS;
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
            final SITransactionId transactionId = assignTransactionId();
            final long beginTimestamp = getBeginTimestamp(transactionId, params.parent);
            transactionStore.recordNewTransaction(transactionId, params, ACTIVE, beginTimestamp, 0L);
            final TransactionId childTransactionId = transactionId;
            transactionStore.addChildToTransaction(params.parent, childTransactionId);
            return childTransactionId;
        } else {
            return createLightweightChildTransaction(parent);
        }
    }

    private SITransactionId assignTransactionId() throws IOException {
        return new SITransactionId(timestampSource.nextTimestamp());
    }

    /**
     * Generate the next sequential timestamp / transaction ID.
     *
     * @return the new transaction ID.
     */
    private long getBeginTimestamp(SITransactionId transactionId, TransactionId parentId) throws IOException {
        if (((SITransactionId) parentId).isRootTransaction()) {
            return transactionId.getId();
        } else {
            return transactionStore.getTimestamp(parentId);
        }
    }

    private long getCommitTimestamp(TransactionId parentId) throws IOException {
        if (((SITransactionId) parentId).isRootTransaction()) {
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
    private TransactionId createLightweightChildTransaction(TransactionId parent) {
        return new SITransactionId(parent.getId(), true);
    }

    @Override
    public void keepAlive(TransactionId transactionId) throws IOException {
        if (!isIndependentReadOnly(transactionId)) {
            transactionStore.recordKeepAlive(transactionId);
        }
    }

    @Override
    public void commit(TransactionId transactionId) throws IOException {
        if (!isIndependentReadOnly(transactionId)) {
            final Transaction transaction = transactionStore.getTransaction(transactionId);
            ensureTransactionActive(transaction);
            performCommit(transaction);
        }
    }

    /**
     * Update the transaction table to show this transaction is committed.
     */
    private void performCommit(Transaction transaction) throws IOException {
        final SITransactionId transactionId = transaction.getTransactionId();
        if (!transactionStore.recordTransactionStatusChange(transactionId, ACTIVE, COMMITTING)) {
            throw new IOException("committing failed");
        }
        Tracer.traceCommitting(transaction.getTransactionId().getId());
        final Long globalCommitTimestamp = getGlobalCommitTimestamp(transaction);
        final long commitTimestamp = getCommitTimestamp(transaction.getParent().getTransactionId());
        if (!transactionStore.recordTransactionEnd(transactionId, commitTimestamp, globalCommitTimestamp, COMMITTING, COMMITTED)) {
            throw new DoNotRetryIOException("commit failed");
        }
    }

    @Override
    public void rollback(TransactionId transactionId) throws IOException {
        if (!isIndependentReadOnly(transactionId)) {
            rollbackDirect(transactionId);
        }
    }

    private void rollbackDirect(TransactionId transactionId) throws IOException {
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
            failDirect(transactionId);
        }
    }

    private void failDirect(TransactionId transactionId) throws IOException {
        transactionStore.recordTransactionStatusChange(transactionId, ACTIVE, ERROR);
    }

    private boolean isIndependentReadOnly(TransactionId transactionId) {
        return ((SITransactionId) transactionId).independentReadOnly;
    }

    // Transaction ID manipulation

    @Override
    public TransactionId transactionIdFromString(String transactionId) {
        return new SITransactionId(transactionId);
    }

    @Override
    public TransactionId transactionIdFromGet(GetOp operation) {
        return dataStore.getTransactionIdFromOperation(operation);
    }

    @Override
    public TransactionId transactionIdFromScan(ScanOp operation) {
        return dataStore.getTransactionIdFromOperation(operation);
    }

    @Override
    public TransactionId transactionIdFromPut(PutOp operation) {
        return dataStore.getTransactionIdFromOperation(operation);
    }

    // Operation initialization. These are expected to be called "client-side" when operations are created.

    @Override
    public void initializeGet(String transactionId, GetOp get) throws IOException {
        initializeOperation(transactionId, get);
    }

    @Override
    public void initializeScan(String transactionId, SScan scan) {
        initializeOperation(transactionId, scan);
    }

    @Override
    public void initializeScan(String transactionId, SScan scan, boolean includeSIColumn) {
        initializeOperation(transactionId, scan, includeSIColumn);
    }

    @Override
    public void initializePut(String transactionId, Object put) {
        initializeOperation(transactionId, put);
    }

    private void initializeOperation(String transactionId, Object operation) {
        initializeOperation(transactionId, operation, false);
    }

    private void initializeOperation(String transactionId, Object operation, boolean includeSIColumn) {
        flagForSITreatment((SITransactionId) transactionIdFromString(transactionId), includeSIColumn, operation);
    }

    @Override
    public PutOp createDeletePut(TransactionId transactionId, Object rowKey) {
        return createDeletePutDirect((SITransactionId) transactionId, rowKey);
    }

    /**
     * Create a "put" operation that will effectively delete a given row.
     */
    private PutOp createDeletePutDirect(SITransactionId transactionId, Object rowKey) {
        final PutOp deletePut = (PutOp) dataLib.newPut(rowKey);
        flagForSITreatment(transactionId, false, deletePut);
        dataStore.setTombstoneOnPut(deletePut, transactionId);
        dataStore.setDeletePutAttribute(deletePut);
        return deletePut;
    }

    @Override
    public boolean isDeletePut(MutationOp put) {
        final Boolean deleteAttribute = dataStore.getDeletePutAttribute(put);
        return (deleteAttribute != null && deleteAttribute);
    }

    /**
     * Set an attribute on the operation that identifies it as needing "snapshot isolation" treatment. This is so that
     * later when the operation comes through for processing we will know how to handle it.
     */
    private void flagForSITreatment(SITransactionId transactionId, boolean includeSIColumn, Object operation) {
        dataStore.setSINeededAttribute(operation, includeSIColumn);
        dataStore.setTransactionId(transactionId, operation);
    }

    // Operation pre-processing. These are to be called "server-side" when we are about to process an operation.


    @Override
    public void preProcessGet(GetOp readOperation) throws IOException {
        preProcessRead(readOperation);
    }

    @Override
    public void preProcessScan(ScanOp readOperation) throws IOException {
        preProcessRead(readOperation);
    }

    private void preProcessRead(SRead read) throws IOException {
        dataLib.setReadTimeRange(read, 0, Long.MAX_VALUE);
        dataLib.setReadMaxVersions(read);
        if (dataStore.isIncludeSIColumn(read)) {
            dataStore.addSIFamilyToRead(read);
        } else {
            dataStore.addSIFamilyToReadIfNeeded(read);
        }
    }

    // Process update operations

    @Override
    public boolean processPut(STable table, RollForwardQueue rollForwardQueue, Object put) throws IOException {
        if (isFlaggedForSITreatment(put)) {
            processPutDirect(table, rollForwardQueue, (PutOp) put);
            return true;
        } else {
            return false;
        }
    }

    private void processPutDirect(STable table, RollForwardQueue rollForwardQueue, PutOp put) throws IOException {
        final SITransactionId transactionId = dataStore.getTransactionIdFromOperation(put);
        final ImmutableTransaction transaction = transactionStore.getImmutableTransaction(transactionId);
        ensureTransactionAllowsWrites(transaction);
        performPut(table, rollForwardQueue, put, transaction);
    }

    private void performPut(STable table, RollForwardQueue rollForwardQueue, PutOp put, ImmutableTransaction transaction)
            throws IOException {
//        if (table instanceof HbRegion) {
//            LOG.error("performPut table = " + ((HbRegion) table).region.toString());
//            LOG.error("performPut transaction = " + transaction.getTransactionId().getTransactionIdString());
//        }
        final Object rowKey = dataLib.getPutKey(put);
        final SRowLock lock = dataWriter.lockRow(table, rowKey);
        Set<Long> dataTransactionsToRollForward;
        // This is the critical section that runs while the row is locked.
        try {
            final Object[] conflictResults = ensureNoWriteConflict(transaction, table, rowKey);
            dataTransactionsToRollForward = (Set<Long>) conflictResults[0];
            Set<Long> conflictingChildren = (Set<Long>) conflictResults[1];
            final Object newPut = createUltimatePut(transaction, lock, put, table);
            dataStore.suppressIndexing(newPut);
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

    private void resolveChildConflicts(STable table, Object put, SRowLock lock, Set<Long> conflictingChildren) throws IOException {
        if (!conflictingChildren.isEmpty()) {
            Object delete = dataStore.copyPutToDelete(put, conflictingChildren);
            dataStore.suppressIndexing(delete);
            dataWriter.delete(table, delete, lock);
        }
    }

    /**
     * While we hold the lock on the row, check to make sure that no transactions have updated the row since the
     * updating transaction started.
     */
    private Object[] ensureNoWriteConflict(ImmutableTransaction updateTransaction, STable table, Object rowKey)
            throws IOException {
        final List dataCommitKeyValues = dataStore.getCommitTimestamp(table, rowKey);
        if (dataCommitKeyValues != null) {
            return checkCommitTimestampsForConflicts(updateTransaction, dataCommitKeyValues);
        }
        return new Object[]{Collections.EMPTY_SET, Collections.EMPTY_SET};
    }

    /**
     * Look at all of the values in the "commitTimestamp" column to see if there are write collisions.
     */
    private Object[] checkCommitTimestampsForConflicts(ImmutableTransaction updateTransaction, List dataCommitKeyValues)
            throws IOException {
        Set<Long> toRollForward = new HashSet<Long>();
        Set<Long> childConflicts = new HashSet<Long>();
        for (Object dataCommitKeyValue : dataCommitKeyValues) {
            final long dataTransactionId = dataLib.getKeyValueTimestamp(dataCommitKeyValue);
            final Object commitTimestampValue = dataLib.getKeyValueValue(dataCommitKeyValue);
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
        return new Object[]{toRollForward, childConflicts};
    }

    /**
     * Look at the last keepAlive timestamp on the transaction, if it is too long in the past then "fail" the
     * transaction. Returns a possibly updated transaction that reflects the call to "fail".
     */
    private Transaction checkTransactionTimeout(Transaction dataTransaction) throws IOException {
        if ((clock.getTime() - dataTransaction.keepAlive) > transactionTimeoutMS) {
            fail(dataTransaction.getTransactionId());
            return transactionStore.getTransaction(dataTransaction.getTransactionId());
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
            Transaction t1 = transactionStore.getTransaction(updateTransaction.getTransactionId().getId());
            final ConflictType conflict = t1.isConflict(dataTransaction);
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
    Object createUltimatePut(ImmutableTransaction transaction, SRowLock lock, PutOp put, STable table) throws IOException {
        final Object rowKey = dataLib.getPutKey(put);
        final Object newPut = dataLib.newPut(rowKey, lock);
        final SITransactionId transactionId = transaction.getTransactionId();
        final long timestamp = transactionId.getId();
        dataStore.copyPutKeyValues(put, newPut, timestamp);
        dataStore.addTransactionIdToPut(newPut, transactionId);
        if (isDeletePut((MutationOp) put)) {
            dataStore.setTombstonesOnColumns(table, timestamp, newPut);
        }
        return newPut;
    }

    // Process read operations

    @Override
    public boolean isFilterNeededGet(GetOp operation) {
        return isFlaggedForSITreatment(operation);
    }

    @Override
    public boolean isFilterNeededScan(ScanOp operation) {
        return isFlaggedForSITreatment(operation);
    }

    @Override
    public boolean isScanIncludeSIColumn(ScanOp read) {
        return dataStore.isIncludeSIColumn(read);
    }

    @Override
    public FilterState newFilterState(TransactionId transactionId) throws IOException {
        return newFilterState(null, transactionId, false);
    }

    @Override
    public FilterState newFilterState(RollForwardQueue rollForwardQueue, TransactionId transactionId, boolean includeSIColumn)
            throws IOException {
        return new SIFilterState(dataLib, dataStore, transactionStore, rollForwardQueue, includeSIColumn,
                transactionStore.getImmutableTransaction(transactionId));
    }

    @Override
    public Filter.ReturnCode filterKeyValue(FilterState filterState, Object keyValue) throws IOException {
        return ((SIFilterState) filterState).filterKeyValue(keyValue);
    }

    @Override
    public ResultType filterResult(FilterState filterState, ResultType result) throws IOException {
        final SDataLib dataLib = dataStore.dataLib;
        final List<Object> filteredCells = new ArrayList<Object>();
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
        if (filteredCells.isEmpty()) {
            return null;
        } else {
            return (ResultType) dataLib.newResult(dataLib.getResultKey(result), filteredCells);
        }
    }

    // Roll-forward / compaction

    @Override
    public void rollForward(STable table, long transactionId, List rows) throws IOException {
        final Transaction transaction = transactionStore.getTransaction(transactionId);
        if (transaction.isCommitted() || transaction.isFailed()) {
            for (Object row : rows) {
                try {
                    if (transaction.isCommitted()) {
                        dataStore.setCommitTimestamp(table, row, transaction.getTransactionId().getId(), transaction.getCommitTimestamp());
                    } else {
                        dataStore.setCommitTimestampToFail(table, row, transaction.getTransactionId().getId());
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
    private boolean isFlaggedForSITreatment(Object put) {
        return dataStore.getSINeededAttribute(put) != null;
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
