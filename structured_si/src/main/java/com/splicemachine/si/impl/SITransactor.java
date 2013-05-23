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
import static com.splicemachine.si.impl.TransactionStatus.LOCAL_COMMIT;
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
        final TransactionParams params = new TransactionParams(null, null, allowWrites, readUncommitted, readCommitted);
        return beginTransactionDirect(params, ACTIVE);
    }

    @Override
    public TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites) throws IOException {
        return beginChildTransaction(parent, dependent, allowWrites, null, null);
    }

    @Override
    public TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites,
                                               Boolean readUncommitted, Boolean readCommitted) throws IOException {
        final ImmutableTransaction parentTransaction = transactionStore.getImmutableTransaction(parent);
        if (parentTransaction.isNested()) {
            throw new RuntimeException("Parent transaction must be a top-level transaction");
        }
        if (dependent || allowWrites) {
            final TransactionParams params = new TransactionParams(parent, dependent, allowWrites, readUncommitted,
                    readCommitted);
            return createHeavyChildTransaction(params);
        } else {
            return createLightweightChildTransaction(parent);
        }
    }

    /**
     * Create a "full-fledged" child transaction. This will get it's own entry in the transaction table.
     */
    private TransactionId createHeavyChildTransaction(TransactionParams params)
            throws IOException {
        final TransactionId childTransactionId = beginTransactionDirect(params, ACTIVE);
        transactionStore.addChildToTransaction(params.parent, childTransactionId);
        return childTransactionId;
    }

    /**
     * Start a transaction. Either a root-level transaction or a nested child transaction.
     */
    private TransactionId beginTransactionDirect(TransactionParams params, TransactionStatus status)
            throws IOException {
        final SITransactionId transactionId = assignTransactionId();
        transactionStore.recordNewTransaction(transactionId, params, status);
        return transactionId;
    }

    /**
     * Generate the next sequential timestamp / transaction ID.
     *
     * @return the new transaction ID.
     */
    private SITransactionId assignTransactionId() {
        return new SITransactionId(timestampSource.nextTimestamp());
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
            commitDirect(transactionId);
        }
    }

    private void commitDirect(TransactionId transactionId) throws IOException {
        final Transaction transaction = transactionStore.getTransaction(transactionId);
        ensureTransactionActive(transaction);
        if (transaction.isNestedDependent()) {
            performLocalCommit(transactionId);
        } else {
            performCommit(transaction);
        }
    }

    /**
     * Nested, dependent children commit locally only. They will finally commit when the root parent transaction commits.
     */
    private void performLocalCommit(TransactionId transactionId) throws IOException {
        // perform "local" commit only within the parent transaction
        if (!transactionStore.recordTransactionStatusChange(transactionId, ACTIVE, LOCAL_COMMIT)) {
            throw new IOException("local commit failed");
        }
    }

    /**
     * Update the transaction table to show this transaction is committed.
     */
    private void performCommit(Transaction transaction) throws IOException {
        final SITransactionId transactionId = transaction.getTransactionId();
        final List<Transaction> childrenToCommit = findChildrenToCommit(transaction);
        if (!transactionStore.recordTransactionStatusChange(transactionId, ACTIVE, COMMITTING)) {
            throw new IOException("committing failed");
        }
        Tracer.traceCommitting(transaction.getTransactionId().getId());
        // TODO: need to sort out how to take child transactions through COMMITTING state, alternatively don't commit
        // TODO: children directly, rather let them inherit their commit status from their parent
        final long endId = timestampSource.nextTimestamp();
        if (!transactionStore.recordTransactionEnd(transactionId, endId, COMMITTING, COMMITTED)) {
            throw new DoNotRetryIOException("commit failed");
        }
        commitAll(childrenToCommit, endId);
    }

    /**
     * Filter the immediate children of the transaction to find the ones that can be committed.
     */
    private List<Transaction> findChildrenToCommit(Transaction transaction) throws IOException {
        final List<Transaction> childrenToCommit = new ArrayList<Transaction>();
        for (Long childId : transaction.getChildren()) {
            final Transaction childTransaction = transactionStore.getTransaction(childId);
            if (childTransaction.isEffectivelyActive()) {
                childrenToCommit.add(childTransaction);
            }
        }
        return childrenToCommit;
    }

    /**
     * Update the transaction table to record all of the transactionIds as committed as of the timestamp.
     */
    private void commitAll(List<Transaction> children, long timestamp) throws IOException {
        for (Transaction childTransaction : children) {
            TransactionStatus expectedStatus = childTransaction.isLocallyCommitted() ? LOCAL_COMMIT : ACTIVE;
            if (!transactionStore.recordTransactionEnd(childTransaction.getTransactionId(), timestamp, expectedStatus, COMMITTED)) {
                throw new IOException("child commit failed");
            }
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
        if (transaction.isActive() && !transaction.isLocallyCommitted()) {
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
    public void initializeScan(String transactionId, SScan scan, boolean siFamilyOnly) {
        initializeOperation(transactionId, scan, siFamilyOnly);
    }

    @Override
    public void initializePut(String transactionId, Object put) {
        initializeOperation(transactionId, put);
    }

    private void initializeOperation(String transactionId, Object operation) {
        initializeOperation(transactionId, operation, false);
    }

    private void initializeOperation(String transactionId, Object operation, boolean siFamilyOnly) {
        flagForSiTreatment((SITransactionId) transactionIdFromString(transactionId), siFamilyOnly, operation);
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
        flagForSiTreatment(transactionId, false, deletePut);
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
    private void flagForSiTreatment(SITransactionId transactionId, boolean siFamilyOnly, Object operation) {
        dataStore.setSiNeededAttribute(operation, siFamilyOnly);
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
        if (dataStore.isSIFamilyOnly(read)) {
            dataStore.addSiFamilyToRead(read);
        } else {
            dataStore.addSiFamilyToReadIfNeeded(read);
        }
    }

    // Process update operations

    @Override
    public boolean processPut(STable table, RollForwardQueue rollForwardQueue, Object put) throws IOException {
        if (isFlaggedForSiTreatment(put)) {
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
            dataTransactionsToRollForward = ensureNoWriteConflict(transaction, table, rowKey);
            final Object newPut = createUltimatePut(transaction, lock, put, table);
            dataStore.suppressIndexing(newPut);
            dataWriter.write(table, newPut, lock);
        } finally {
            dataWriter.unLockRow(table, lock);
        }
        dataStore.recordRollForward(rollForwardQueue, transaction, rowKey);
        for (Long id : dataTransactionsToRollForward) {
            dataStore.recordRollForward(rollForwardQueue, id, rowKey);
        }
    }

    /**
     * While we hold the lock on the row, check to make sure that no transactions have updated the row since the
     * updating transaction started.
     */
    private Set<Long> ensureNoWriteConflict(ImmutableTransaction updateTransaction, STable table, Object rowKey)
            throws IOException {
        final List dataCommitKeyValues = dataStore.getCommitTimestamp(table, rowKey);
        if (dataCommitKeyValues != null) {
            return checkCommitTimestampsForConflicts(updateTransaction, dataCommitKeyValues);
        }
        return Collections.EMPTY_SET;
    }

    /**
     * Look at all of the values in the "commitTimestamp" column to see if there are write collisions.
     */
    private Set<Long> checkCommitTimestampsForConflicts(ImmutableTransaction updateTransaction, List dataCommitKeyValues)
            throws IOException {
        Set<Long> toRollForward = new HashSet<Long>();
        for (Object dataCommitKeyValue : dataCommitKeyValues) {
            final long dataTransactionId = dataLib.getKeyValueTimestamp(dataCommitKeyValue);
            final Object commitTimestampValue = dataLib.getKeyValueValue(dataCommitKeyValue);
            if (dataStore.isSiNull(commitTimestampValue)) {
                Transaction dataTransaction = transactionStore.getTransaction(dataTransactionId);
                dataTransaction = checkTransactionTimeout(dataTransaction);
                checkTransactionConflict(updateTransaction, dataTransaction);
                toRollForward.add(dataTransactionId);
            } else if (dataStore.isSiFail(commitTimestampValue)) {
            } else {
                final long dataCommitTimestamp = (Long) dataLib.decode(commitTimestampValue, Long.class);
                if (dataCommitTimestamp > updateTransaction.beginTimestamp) {
                    failOnWriteConflict(updateTransaction);
                }
            }
        }
        return toRollForward;
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
    private void checkTransactionConflict(ImmutableTransaction updateTransaction, Transaction dataTransaction)
            throws IOException {
        if (dataTransaction.committedAfter(updateTransaction)) {
            // if the row was updated after this update's transaction started then fail
            failOnWriteConflict(updateTransaction);
        } else if (dataTransaction.isEffectivelyActive() && !dataTransaction.isEffectivelyPartOfTransaction(updateTransaction)) {
            // if the row was written by an active transaction, that is not part of this update then fail
            failOnWriteConflict(updateTransaction);
        }
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
        return isFlaggedForSiTreatment(operation);
    }

    @Override
    public boolean isFilterNeededScan(ScanOp operation) {
        return isFlaggedForSiTreatment(operation);
    }

    @Override
    public boolean isScanSIFamilyOnly(ScanOp read) {
        return dataStore.isSIFamilyOnly(read);
    }

    @Override
    public FilterState newFilterState(TransactionId transactionId) throws IOException {
        return newFilterState(null, transactionId, false);
    }

    @Override
    public FilterState newFilterState(RollForwardQueue rollForwardQueue, TransactionId transactionId, boolean siOnly) throws IOException {
        return new SIFilterState(dataLib, dataStore, transactionStore, rollForwardQueue, siOnly,
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
                        dataStore.setCommitTimestamp(table, row, transaction.beginTimestamp, transaction.commitTimestamp);
                    } else {
                        dataStore.setCommitTimestampToFail(table, row, transaction.beginTimestamp);
                    }
                    Tracer.trace(row);
                } catch (NotServingRegionException e) {
                    // If the region split and the row is not here, then just skip it
                }
            }
        }
        Tracer.traceTransaction(transactionId);
    }

    @Override
    public SICompactionState newCompactionState() {
        return new SICompactionState(dataLib, dataStore, transactionStore);
    }

    // Helpers

    /**
     * Is this operation supposed to be handled by "snapshot isolation".
     */
    private boolean isFlaggedForSiTreatment(Object put) {
        return dataStore.getSiNeededAttribute(put) != null;
    }

    /**
     * Throw an exception if the transaction is not active.
     */
    private void ensureTransactionActive(Transaction transaction) throws IOException {
        if (!transaction.isEffectivelyActive()) {
            throw new DoNotRetryIOException("transaction is not ACTIVE: " +
                    transaction.getTransactionId().getTransactionIdString());
        }
    }

    /**
     * Throw an exception if this is a read-only transaction.
     */
    private void ensureTransactionAllowsWrites(ImmutableTransaction transaction) throws IOException {
        if (transaction.isReadOnly()) {
            throw new DoNotRetryIOException("transaction is read only");
        }
    }

}
