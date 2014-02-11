package com.splicemachine.si.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.*;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import javax.annotation.concurrent.Immutable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.*;

import static com.splicemachine.si.api.TransactionStatus.ACTIVE;
import static com.splicemachine.si.api.TransactionStatus.COMMITTED;
import static com.splicemachine.si.api.TransactionStatus.COMMITTING;
import static com.splicemachine.si.api.TransactionStatus.ERROR;
import static com.splicemachine.si.api.TransactionStatus.ROLLED_BACK;

/**
 * Central point of implementation of the "snapshot isolation" MVCC algorithm that provides transactions across atomic
 * row updates in the underlying store. This is the core brains of the SI logic.
 */
public class SITransactor<Table, OperationWithAttributes, Mutation extends OperationWithAttributes, Put extends Mutation, Get extends OperationWithAttributes,
        Scan extends OperationWithAttributes, Result, KeyValue, Data, Hashable extends Comparable,
        Delete extends OperationWithAttributes, Lock, OperationStatus, Scanner>
        implements Transactor<Table, Put, Get, Scan, Mutation, OperationStatus, Result, KeyValue, Data, Hashable, Lock> {
    static final Logger LOG = Logger.getLogger(SITransactor.class);
    public static final int MAX_ACTIVE_COUNT = 1000;

    private final TimestampSource timestampSource;
    private final SDataLib<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock, OperationStatus> dataLib;
    private final STableWriter<Table, Mutation, Put, Delete, Data, Lock, org.apache.hadoop.hbase.regionserver.OperationStatus> dataWriter;
    private final DataStore<Data, Hashable, Result, KeyValue, OperationWithAttributes, Mutation, Put, Delete, Get, Scan, Table, Lock, OperationStatus, Scanner> dataStore;
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
        return beginChildTransaction(Transaction.rootTransaction.getTransactionId(), true, allowWrites, false, readUncommitted,
                readCommitted, null);
    }

    @Override
    public TransactionId beginChildTransaction(TransactionId parent, boolean allowWrites) throws IOException {
        return beginChildTransaction(parent, true, allowWrites);
    }

    @Override
    public TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites) throws IOException {
        return beginChildTransaction(parent, dependent, allowWrites, false, null, null, null);
    }

    @Override
    public TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites,
                                               boolean additive, Boolean readUncommitted, Boolean readCommitted,
                                               TransactionId transactionToCommit) throws IOException {
        Long timestamp = commitIfNeeded(transactionToCommit);
        if (allowWrites || readCommitted != null || readUncommitted != null || timestamp != null) {
            final TransactionParams params = new TransactionParams(parent, dependent, allowWrites, additive, readUncommitted, readCommitted);
            if (timestamp == null) {
                timestamp = assignTransactionId();
            }
            final long beginTimestamp = generateBeginTimestamp(timestamp, params.parent.getId());
            transactionStore.recordNewTransaction(timestamp, params, ACTIVE, beginTimestamp, 0L);
            listener.beginTransaction(!parent.isRootTransaction());
            return new TransactionId(timestamp);
        } else {
            return createLightweightChildTransaction(parent.getId());
        }
    }

    private Long commitIfNeeded(TransactionId transactionToCommit) throws IOException {
        if (transactionToCommit == null) {
            return null;
        } else {
            return commitAndReturnTimestamp(transactionToCommit);
        }
    }

    private Long commitAndReturnTimestamp(TransactionId transactionToCommit) throws IOException {
        final ImmutableTransaction immutableTransaction = transactionStore.getImmutableTransaction(transactionToCommit);
        if (!immutableTransaction.getImmutableParent().isRootTransaction()) {
            throw new RuntimeException("Cannot begin a child transaction at the time a non-root transaction commits: "
                    + transactionToCommit.getTransactionIdString());
        }
        commit(transactionToCommit);
        final Transaction transaction = transactionStore.getTransaction(transactionToCommit.getId());
        final Long commitTimestampDirect = transaction.getCommitTimestampDirect();
        if (commitTimestampDirect.equals(transaction.getEffectiveCommitTimestamp())) {
            return commitTimestampDirect;
        } else {
            throw new RuntimeException("commit times did not match");
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

    @Override
    public TransactionStatus getTransactionStatus(TransactionId transactionId) throws IOException {
        return transactionStore.getTransaction(transactionId).status;
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
        initializeOperation(transactionId, get, true);
    }

    @Override
    public void initializeScan(String transactionId, Scan scan, boolean includeSIColumn) {
        initializeOperation(transactionId, scan, includeSIColumn);
    }

    @Override
    public void initializePut(String transactionId, Put put) {
        initializePut(transactionId,put,true);
    }

    @Override
    public void initializePut(String transactionId, Put put, boolean addPlaceHolderColumnToEmptyPut) {
        initializeOperation(transactionId, put);
        if (addPlaceHolderColumnToEmptyPut)
        	dataStore.addPlaceHolderColumnToEmptyPut(put);
    }

    private void initializeOperation(String transactionId, OperationWithAttributes operation) {
        initializeOperation(transactionId, operation, false);
    }

    private void initializeOperation(String transactionId, OperationWithAttributes operation, boolean includeSIColumn) {
        flagForSITreatment(transactionIdFromString(transactionId).getId(), includeSIColumn, operation);
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
        flagForSITreatment(transactionId, false, deletePut);
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
                                    OperationWithAttributes operation) {
        dataStore.setSINeededAttribute(operation, includeSIColumn);
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
        if (isFlaggedForSITreatment(put)) {
            processPutDirect(table, rollForwardQueue, put);
            return true;
        } else {
            return false;
        }
    }

    private void processPutDirect(Table table, RollForwardQueue<Data, Hashable> rollForwardQueue, Put put) throws IOException {
        final Mutation[] mutations = (Mutation[]) Array.newInstance(put.getClass(), 1);
        mutations[0] = put;
        final OperationStatus[] operationStatuses = processPutBatch(table, rollForwardQueue, mutations);
        if (!((org.apache.hadoop.hbase.regionserver.OperationStatus) operationStatuses[0]).getOperationStatusCode().equals(HConstants.OperationStatusCode.SUCCESS)) {
            throw new RuntimeException("operation not successful: " + operationStatuses[0]);
        }
    }

    @Override
    public OperationStatus[] processPutBatch(Table table, RollForwardQueue<Data, Hashable> rollForwardQueue, Mutation[] mutations)
            throws IOException {
        if (mutations.length == 0) {
            //short-circuit special case of empty batch
						//noinspection unchecked
						return dataStore.writeBatch(table, new Pair[0]);
        }

        // TODO add back the check if it is needed for DDL operations
        // checkPermission(table, mutations);

        Map<Hashable, Lock> locks = Maps.newHashMapWithExpectedSize(1);
        Map<Hashable, List<PutInBatch<Data, Put>>> putInBatchMap = Maps.newHashMapWithExpectedSize(mutations.length);
        try {
            @SuppressWarnings("unchecked") final Pair<Mutation, Lock>[] mutationsAndLocks = new Pair[mutations.length];
            obtainLocksForPutBatchAndPrepNonSIPuts(table, mutations, locks, putInBatchMap, mutationsAndLocks);

            @SuppressWarnings("unchecked") final Set<Long>[] conflictingChildren = new Set[mutations.length];
            dataStore.startLowLevelOperation(table);
            try {
                checkConflictsForPutBatch(table, rollForwardQueue, locks, putInBatchMap, mutationsAndLocks, conflictingChildren);
            } finally {
                dataStore.closeLowLevelOperation(table);
            }

            final OperationStatus[] status = dataStore.writeBatch(table, mutationsAndLocks);

            resolveConflictsForPutBatch(table, mutations, locks, conflictingChildren, status);

            return status;
        } finally {
            releaseLocksForPutBatch(table, locks);
        }
    }

		@Override
		public OperationStatus[] processKvBatch(Table table, RollForwardQueue<Data,Hashable> rollForwardQueue, Collection<KVPair> mutations, String txnId) throws IOException {
				if(mutations.size()<=0)
						//noinspection unchecked
						return dataStore.writeBatch(table,new Pair[]{});

				//ensure the transaction is in good shape
				TransactionId txn = transactionIdFromString(txnId);
				ImmutableTransaction transaction = transactionStore.getImmutableTransaction(txn);
				ensureTransactionAllowsWrites(transaction);

				Pair<KVPair,Lock>[] lockPairs = null;
				try {
						lockPairs = obtainLocksForPutBatchAndPrepNonSIKVs(table, mutations, transaction);

						@SuppressWarnings("unchecked") final Set<Long>[] conflictingChildren = new Set[mutations.size()];
						dataStore.startLowLevelOperation(table);
						Pair<Mutation,Lock>[] writes;
						try {
								writes = checkConflictsForKvBatch(table, rollForwardQueue, lockPairs, conflictingChildren, transaction);
						} finally {
								dataStore.closeLowLevelOperation(table);
						}

						final OperationStatus[] status = dataStore.writeBatch(table, writes);

						resolveConflictsForKvBatch(table, writes, conflictingChildren, status);

						return status;
				} finally {
						releaseLocksForKvBatch(table, lockPairs);
				}
		}

		private void checkPermission(Table table, Mutation[] mutations) throws IOException {
        final String tableName = dataStore.getTableName(table);
        for (TransactionId t : getTransactionIds(mutations)) {
            transactionStore.confirmPermission(t, tableName);
        }
    }

    private Set<TransactionId> getTransactionIds(Mutation[] mutations) {
        Set<TransactionId> writingTransactions = new HashSet<TransactionId>();
        for (Mutation m : mutations) {
            writingTransactions.add(dataStore.getTransactionIdFromOperation(m));
        }
        return writingTransactions;
    }

		private void releaseLocksForKvBatch(Table table, Pair<KVPair,Lock>[] locks){
				if(locks==null) return;
				for(Pair<KVPair,Lock>lock:locks){
						try {
								dataWriter.unLockRow(table,lock.getSecond());
						} catch (IOException e) {
								LOG.error("Unable to unlock row "+ Bytes.toStringBinary(lock.getFirst().getRow()),e);
								//ignore otherwise
						}
				}
		}

    private void releaseLocksForPutBatch(Table table, Map<Hashable, Lock> locks) {
        for (Lock lock : locks.values()) {
            if (lock != null) {
                try {
                    dataWriter.unLockRow(table, lock);
                } catch (Exception ex) {
                    LOG.error("Exception while cleaning up locks", ex);
                    // ignore
                }
            }
        }
    }

		private void resolveConflictsForKvBatch(Table table, Pair<Mutation,Lock>[] mutations, Set<Long>[] conflictingChildren,OperationStatus[] status){
				for(int i=0;i<mutations.length;i++){
						try{
								Put put = (Put)mutations[i].getFirst();
								Lock lock = mutations[i].getSecond();
								resolveChildConflicts(table,put,lock,conflictingChildren[i]);
						}catch(Exception ex){
								status[i]= dataLib.newFailStatus();
						}
				}
		}

    private void resolveConflictsForPutBatch(Table table, Mutation[] mutations, Map<Hashable, Lock> locks, Set<Long>[] conflictingChildren, OperationStatus[] status) {
        for (int i = 0; i < mutations.length; i++) {
            try {
                if (isFlaggedForSITreatment(mutations[i])) {
                    Put put = (Put) mutations[i];
                    Lock lock = locks.get(hasher.toHashable(dataLib.getPutKey(put)));
                    resolveChildConflicts(table, put, lock, conflictingChildren[i]);
                }
            } catch (Exception ex) {
                LOG.error("Exception while post processing batch put", ex);
                status[i] = dataLib.newFailStatus();
            }
        }
    }

		private Pair<Mutation,Lock>[] checkConflictsForKvBatch(Table table, RollForwardQueue<Data,Hashable> rollForwardQueue,
																								Pair<KVPair,Lock>[] dataAndLocks,
																								Set<Long>[] conflictingChildren,ImmutableTransaction transaction) throws IOException{
				@SuppressWarnings("unchecked") Pair<Mutation,Lock>[] finalPuts = new Pair[dataAndLocks.length];
				for(int i=0;i<dataAndLocks.length;i++){
						ConflictResults conflictResults;
						if(unsafeWrites)
							conflictResults = ConflictResults.NO_CONFLICT;
						else{
								List<KeyValue>[] possibleConflicts = dataStore.getCommitTimestampsAndTombstonesSingle(table,(Data)dataAndLocks[i].getFirst().getRow());
								conflictResults = ensureNoWriteConflict(transaction,possibleConflicts);
						}
						conflictingChildren[i] = conflictResults.childConflicts;
						finalPuts[i] = Pair.newPair(getMutationToRun(table, rollForwardQueue, dataAndLocks[i].getFirst(), transaction, conflictResults),dataAndLocks[i].getSecond());
				}
				return finalPuts;
		}

		private static final boolean unsafeWrites = Boolean.getBoolean("splice.unsafe.writes");
    private void checkConflictsForPutBatch(Table table, RollForwardQueue<Data, Hashable> rollForwardQueue, Map<Hashable, Lock> locks, Map<Hashable, List<PutInBatch<Data, Put>>> putInBatchMap, Pair<Mutation, Lock>[] mutationsAndLocks, Set<Long>[] conflictingChildren) throws IOException {
        final SortedSet<Hashable> keys = new TreeSet<Hashable>(putInBatchMap.keySet());
        for (Hashable hashableRowKey : keys) {
            for (PutInBatch<Data, Put> putInBatch : putInBatchMap.get(hashableRowKey)) {
                ConflictResults conflictResults;
                if (unsafeWrites) {
                    conflictResults = new ConflictResults(Collections.<Long>emptySet(), Collections.<Long>emptySet(), false);
                } else {
                    final List<KeyValue>[] values = dataStore.getCommitTimestampsAndTombstonesSingle(table, putInBatch.rowKey);
                    conflictResults = ensureNoWriteConflict(putInBatch.transaction, values);
                }
                final PutToRun<Mutation, Lock> putToRun = getMutationLockPutToRun(table, rollForwardQueue, putInBatch.put, putInBatch.transaction, putInBatch.rowKey, locks.get(hashableRowKey), conflictResults);
                mutationsAndLocks[putInBatch.index] = putToRun.putAndLock;
                conflictingChildren[putInBatch.index] = putToRun.conflictingChildren;
            }
        }
    }


    private void obtainLocksForPutBatchAndPrepNonSIPuts(Table table, Mutation[] mutations, Map<Hashable, Lock> locks, Map<Hashable,
            List<PutInBatch<Data, Put>>> putInBatchMap, Pair<Mutation, Lock>[] mutationsAndLocks) throws IOException {
        for (int i = 0; i < mutations.length; i++) {
            if (isFlaggedForSITreatment(mutations[i])) {
                obtainLockForPutBatch(table, mutations[i], locks, putInBatchMap, i);
            } else {
                mutationsAndLocks[i] = new Pair<Mutation, Lock>(mutations[i], null);
            }
        }
    }

		private Pair<KVPair,Lock>[] obtainLocksForPutBatchAndPrepNonSIKVs(Table table,Collection<KVPair> mutations,ImmutableTransaction transaction) throws IOException {
				@SuppressWarnings("unchecked") Pair<KVPair,Lock>[] mutationsAndLocks = new Pair[mutations.size()];

				int i=0;
				for(KVPair pair:mutations){
						mutationsAndLocks[i] = Pair.newPair(pair,dataWriter.lockRow(table,(Data)pair.getRow()));
						i++;
				}
				return mutationsAndLocks;
		}


		private void obtainLockForPutBatch(Table table, Mutation mutation, Map<Hashable, Lock> locks, Map<Hashable, List<PutInBatch<Data, Put>>> putInBatchMap, int i) throws IOException {
        final Put put = (Put) mutation;
        final TransactionId transactionId = dataStore.getTransactionIdFromOperation(put);
        final ImmutableTransaction transaction = transactionStore.getImmutableTransaction(transactionId);
        ensureTransactionAllowsWrites(transaction);
        final Data rowKey = dataLib.getPutKey(put);

        final Hashable hashableRowKey = obtainLock(locks, table, rowKey);
        List<PutInBatch<Data, Put>> putsInBatch = putInBatchMap.get(hashableRowKey);
        final PutInBatch newPutInBatch = new PutInBatch(transaction, rowKey, i, put);
        if (putsInBatch == null) {
            putsInBatch = new ArrayList<PutInBatch<Data, Put>>();
            putsInBatch.add(newPutInBatch);
            putInBatchMap.put(hashableRowKey, putsInBatch);
        } else {
            putsInBatch.add(newPutInBatch);
        }
    }


		private Mutation getMutationToRun(Table table, RollForwardQueue<Data,Hashable> rollForwardQueue, KVPair kvPair,ImmutableTransaction transaction,ConflictResults conflictResults) throws IOException{
				long txnIdLong = transaction.getLongTransactionId();
				Put newPut = (Put) kvPair.toPut(txnIdLong);
				dataStore.suppressIndexing(newPut);
				dataStore.addTransactionIdToPutKeyValues(newPut, txnIdLong);
				if(kvPair.getType()== KVPair.Type.DELETE)
						dataStore.setTombstoneOnPut(newPut,txnIdLong);
				else if(conflictResults.hasTombstone)
						dataStore.setAntiTombstoneOnPut(newPut, txnIdLong);

				Data row = (Data) kvPair.getRow();
				dataStore.recordRollForward(rollForwardQueue,txnIdLong, row,false);
				for(Long txnIdToRollForward : conflictResults.toRollForward){
					dataStore.recordRollForward(rollForwardQueue,txnIdToRollForward,row,false);
				}
				return newPut;
		}


		private Mutation getMutationToRun(Table table, RollForwardQueue<Data,Hashable> rollForwardQueue, KVPair kvPair,ImmutableTransaction transaction,ConflictResults conflictResults) throws IOException{
				long txnIdLong = transaction.getLongTransactionId();
				Put newPut = (Put) kvPair.toPut(txnIdLong);
				dataStore.suppressIndexing(newPut);
				dataStore.addTransactionIdToPutKeyValues(newPut, txnIdLong);
				if(kvPair.getType()== KVPair.Type.DELETE)
						dataStore.setTombstoneOnPut(newPut,txnIdLong);
				else if(conflictResults.hasTombstone)
						dataStore.setAntiTombstoneOnPut(newPut, txnIdLong);

				Data row = (Data) kvPair.getRow();
				dataStore.recordRollForward(rollForwardQueue,txnIdLong, row,false);
				if(conflictResults.toRollForward!=null){
						for(Long txnIdToRollForward : conflictResults.toRollForward){
								dataStore.recordRollForward(rollForwardQueue,txnIdToRollForward,row,false);
						}
				}
				return newPut;
		}

    private PutToRun<Mutation, Lock> getMutationLockPutToRun(Table table, RollForwardQueue<Data, Hashable> rollForwardQueue, Put put, ImmutableTransaction transaction, Data rowKey, Lock lock, ConflictResults conflictResults) throws IOException {
        final Put newPut = createUltimatePut(transaction.getLongTransactionId(), lock, put, table,
                conflictResults.hasTombstone);
        dataStore.suppressIndexing(newPut);
        dataStore.recordRollForward(rollForwardQueue, transaction.getLongTransactionId(), rowKey, false);
				if(conflictResults.toRollForward!=null){
						for (Long transactionIdToRollForward : conflictResults.toRollForward) {
								dataStore.recordRollForward(rollForwardQueue, transactionIdToRollForward, rowKey, false);
						}
				}
        return new PutToRun<Mutation, Lock>(new Pair<Mutation, Lock>(newPut, lock), conflictResults.childConflicts);
    }

    private Hashable obtainLock(Map<Hashable, Lock> locks, Table table, Data rowKey) throws IOException {
        final Hashable hashableRowKey = hasher.toHashable(rowKey);
        Lock lock = locks.get(hashableRowKey);
        if (lock == null) {
            lock = dataWriter.lockRow(table, rowKey);
            locks.put(hashableRowKey, lock);
        }
        return hashableRowKey;
    }

    private void resolveChildConflicts(Table table, Put put, Lock lock, Set<Long> conflictingChildren) throws IOException {
        if (conflictingChildren!=null && !conflictingChildren.isEmpty()) {
            Delete delete = dataStore.copyPutToDelete(put, conflictingChildren);
            dataStore.suppressIndexing(delete);
            dataWriter.delete(table, delete, lock);
        }
    }

    /**
     * While we hold the lock on the row, check to make sure that no transactions have updated the row since the
     * updating transaction started.
     */
    private ConflictResults ensureNoWriteConflict(ImmutableTransaction updateTransaction, List<KeyValue>[] values)
            throws IOException {
        final ConflictResults timestampConflicts = checkTimestampsHandleNull(updateTransaction, values[1]);
        final List<KeyValue> tombstoneValues = values[0];
        boolean hasTombstone = hasCurrentTransactionTombstone(updateTransaction.getLongTransactionId(), tombstoneValues);
        return new ConflictResults(timestampConflicts.toRollForward, timestampConflicts.childConflicts, hasTombstone);
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

    private ConflictResults checkTimestampsHandleNull(ImmutableTransaction updateTransaction, List<KeyValue> dataCommitKeyValues) throws IOException {
        if (dataCommitKeyValues == null) {
            return new ConflictResults(Collections.<Long>emptySet(),Collections.<Long>emptySet(), null);
        } else {
            return checkCommitTimestampsForConflicts(updateTransaction, dataCommitKeyValues);
        }
    }

    /**
     * Look at all of the values in the "commitTimestamp" column to see if there are write collisions.
     */
    private ConflictResults checkCommitTimestampsForConflicts(ImmutableTransaction updateTransaction, List<KeyValue> dataCommitKeyValues)
            throws IOException {
//        final Set<Long> toRollForward = new HashSet<Long>();
//        final Set<Long> childConflicts = new HashSet<Long>();
				@SuppressWarnings("unchecked") Set<Long>[] conflicts = new Set[2];
        for (KeyValue dataCommitKeyValue : dataCommitKeyValues) {
            checkCommitTimestampForConflict(updateTransaction, conflicts, dataCommitKeyValue);
        }
        return new ConflictResults(conflicts[0], conflicts[1], null);
    }

    private void checkCommitTimestampForConflict(ImmutableTransaction updateTransaction,Set<Long>[] conflicts, KeyValue dataCommitKeyValue)
            throws IOException {
        final long dataTransactionId = dataLib.getKeyValueTimestamp(dataCommitKeyValue);
        if (!updateTransaction.sameTransaction(dataTransactionId)) {
            final Data commitTimestampValue = dataLib.getKeyValueValue(dataCommitKeyValue);
            if (dataStore.isSINull(dataCommitKeyValue)) {
                // Unknown transaction status
                final Transaction dataTransaction = transactionStore.getTransaction(dataTransactionId);
                if (dataTransaction.getEffectiveStatus().isFinished()) {
                    // Transaction is now in a final state so asynchronously update the data row with the status
										if(conflicts[0]==null)
												conflicts[0] = Sets.newHashSetWithExpectedSize(1);
                    conflicts[0].add(dataTransactionId);
                }
                if(dataTransaction.getEffectiveStatus()== TransactionStatus.ROLLED_BACK)
                    return; //can't conflict with a rolled back transaction
                final ConflictType conflictType = checkTransactionConflict(updateTransaction, dataTransaction);
                switch (conflictType) {
                    case CHILD:
												if(conflicts[1]==null)
														conflicts[1] = Sets.newHashSetWithExpectedSize(1);
                        conflicts[1].add(dataTransactionId);
                        break;
                    case SIBLING:
                        if (doubleCheckConflict(updateTransaction, dataTransaction)) {
                            throw new WriteConflict("Write conflict detected between active transactions "+ dataTransactionId+" and "+ updateTransaction.getTransactionId());
                        }
                        break;
                }
            } else if (dataStore.isSIFail(dataCommitKeyValue)) {
                // Can't conflict with failed transaction.
            } else {
                // Committed transaction
                final long dataCommitTimestamp = (Long) dataLib.decode(commitTimestampValue, Long.class);
                if (dataCommitTimestamp > updateTransaction.getEffectiveBeginTimestamp()) {
                    throw new WriteConflict("Write transaction "+ updateTransaction.getTransactionId()+" conflicts with committed transaction " + dataTransactionId);
                }
            }
        }
    }

    /**
     * @param updateTransaction the transaction for the new change
     * @param dataTransaction the transaction for the existing data
     * @return true if there is a conflict
     * @throws IOException if something goes wrong fetching transaction information
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
        if (updateTransaction.sameTransaction(dataTransaction) || updateTransaction.isAdditive() || dataTransaction.isAdditive()) {
            return ConflictType.NONE;
        } else {
            return updateTransaction.isInConflictWith(dataTransaction, transactionSource);
        }
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
    public IFilterState newFilterState(TransactionId transactionId) throws IOException {
        return newFilterState(null, transactionId, false);
    }

    @Override
    public IFilterState newFilterState(RollForwardQueue rollForwardQueue, TransactionId transactionId,
                                       boolean includeSIColumn)
            throws IOException {
        return new FilterState(dataLib, dataStore, transactionStore, rollForwardQueue, includeSIColumn,
                transactionStore.getImmutableTransaction(transactionId));
    }

    @Override
    public IFilterState newFilterStatePacked(String tableName, RollForwardQueue<Data, Hashable> rollForwardQueue, EntryPredicateFilter predicateFilter,
                                             TransactionId transactionId, boolean includeSIColumn) throws IOException {
        return new FilterStatePacked(tableName, dataLib, dataStore,
                (FilterState) newFilterState(rollForwardQueue, transactionId, includeSIColumn),
                new HRowAccumulator(predicateFilter, new EntryDecoder(KryoPool.defaultPool())));
    }

    @Override
    public Filter.ReturnCode filterKeyValue(IFilterState filterState, KeyValue keyValue) throws IOException {
        return filterState.filterKeyValue(keyValue);
    }

    @Override
    public void filterNextRow(IFilterState filterState) {
        filterState.nextRow();
    }

    @Override
    public Result filterResult(IFilterState<KeyValue> filterState, Result result) throws IOException {
        final SDataLib<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock, OperationStatus> dataLib = dataStore.dataLib;
        final List<KeyValue> filteredCells = new ArrayList<KeyValue>();
        final List<KeyValue> keyValues = dataLib.listResult(result);
        if (keyValues != null) {
            Data qualifierToSkip = null;
            Data familyToSkip = null;
            Data currentRowKey = null;
            for (KeyValue keyValue : keyValues) {
                final Data rowKey = dataLib.getKeyValueRow(keyValue);
                if (currentRowKey == null || !dataLib.valuesEqual(currentRowKey, rowKey)) {
                    currentRowKey = rowKey;
                    filterNextRow(filterState);
                }
                if (familyToSkip != null
                        && dataLib.valuesEqual(familyToSkip, dataLib.getKeyValueFamily(keyValue))
                        && dataLib.valuesEqual(qualifierToSkip, dataLib.getKeyValueQualifier(keyValue))) {
                    // skipping to next column
                } else {
                    familyToSkip = null;
                    qualifierToSkip = null;
                    boolean nextRow = false;
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
                        case NEXT_ROW:
                            nextRow = true;
                            break;
                    }
                    if (nextRow) {
                        break;
                    }
                }
            }
        }
        final KeyValue finalKeyValue = filterState.produceAccumulatedKeyValue();
        if (finalKeyValue != null) {
            filteredCells.add(finalKeyValue);
        }
        if (filteredCells.isEmpty()) {
            return null;
        } else {
            return dataLib.newResult(dataLib.getResultKey(result), filteredCells);
        }
    }

    // Roll-forward / compaction

    @Override
    public Boolean rollForward(Table table, long transactionId, List<Data> rows) throws IOException {
        final Transaction transaction = transactionStore.getTransaction(transactionId);
        final Boolean isFinished = transaction.getEffectiveStatus().isFinished();
				boolean isCommitted = transaction.getEffectiveStatus().isCommitted();
        if (isFinished && isCommitted) {
            for (Data row : rows) {
                try {
//                    if (transaction.getEffectiveStatus().isCommitted()) {
                        dataStore.setCommitTimestamp(table, row, transaction.getLongTransactionId(), transaction.getEffectiveCommitTimestamp());
//                    }
//										else {
//                        dataStore.setCommitTimestampToFail(table, row, transaction.getLongTransactionId());
//                    }
                    Tracer.traceRowRollForward(row);
                } catch (NotServingRegionException e) {
                    // If the region split and the row is not here, then just skip it
                }
            }
        }
        Tracer.traceTransactionRollForward(transactionId);
        return isFinished;
    }

    @Override
    public SICompactionState newCompactionState() {
        return new SICompactionState(dataLib, dataStore, transactionStore);
    }

    @Override
    public void compact(SICompactionState compactionState, List<KeyValue> rawList, List<KeyValue> results) throws IOException {
        compactionState.mutate(rawList, results);
    }

    @Override
    public DDLFilter newDDLFilter(String transactionId) throws IOException {
        return new DDLFilter(
                transactionStore.getTransaction(transactionIdFromString(transactionId)),
                transactionStore
                );
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
        /*
         * If the transaction is not finished, then it's active, so no worries.
         *
         * If it's committed, or COMMITTING, then it's also considered still active, so we
         * ignore it
         */
        if(transaction.status.isFinished()
                && transaction.status!=TransactionStatus.COMMITTED
                && transaction.status!=TransactionStatus.COMMITTING){
            String txnIdStr = transaction.getTransactionId().getTransactionIdString();
            throw new DoNotRetryIOException("transaction " + txnIdStr + " is not ACTIVE. State is " + transaction.status);
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

    @Override
    public List<TransactionId> getActiveTransactionIds(TransactionId max) throws IOException {
        final long currentMin = timestampSource.retrieveTimestamp();
        final TransactionParams missingParams = new TransactionParams(Transaction.rootTransaction.getTransactionId(),
                true, false, false, false, false);
        final List<Transaction> oldestActiveTransactions = transactionStore.getOldestActiveTransactions(
                currentMin, max.getId(), MAX_ACTIVE_COUNT, missingParams, TransactionStatus.ERROR);
        final List<TransactionId> result = new ArrayList<TransactionId>(oldestActiveTransactions.size());
        if (!oldestActiveTransactions.isEmpty()) {
            final long oldestId = oldestActiveTransactions.get(0).getTransactionId().getId();
            if (oldestId > currentMin) {
                timestampSource.rememberTimestamp(oldestId);
            }
            final TransactionId youngestId = oldestActiveTransactions.get(oldestActiveTransactions.size() - 1).getTransactionId();
            if (youngestId.equals(max)) {
                for (Transaction t : oldestActiveTransactions) {
                    result.add(t.getTransactionId());
                }
            } else {
                throw new RuntimeException("expected max id of " + max + " but was " + youngestId);
            }
        }
        return result;
    }

    @Override
    public boolean forbidWrites(String tableName, TransactionId transactionId) throws IOException {
        return transactionStore.forbidPermission(tableName, transactionId);
    }

		public static class Builder<Data,
						Result,KeyValue,OperationWithAttributes,Mutation extends OperationWithAttributes,
						Put extends Mutation,Delete extends OperationWithAttributes,Get extends OperationWithAttributes,Scan extends OperationWithAttributes,
						Lock,OperationStatus,Table,Hashable extends Comparable<Hashable>,Scanner>{
				private TimestampSource timestampSource;
				private SDataLib<Data,
								Result,  KeyValue,  OperationWithAttributes,
								Put,  Delete,  Get,  Scan,
								Lock,  OperationStatus> dataLib;
				private STableWriter<Table, Mutation, Put, Delete, Data, Lock, org.apache.hadoop.hbase.regionserver.OperationStatus> dataWriter;
				private DataStore<Data,
								Hashable,
								Result, KeyValue, OperationWithAttributes,
								Mutation, Put, Delete, Get, Scan,
								Table, Lock, OperationStatus, Scanner> dataStore;
				private TransactionStore transactionStore;
				private Clock clock;
				private int transactionTimeoutMS;
				private Hasher<Data, Hashable> hasher;
				private TransactorListener listener;
				private TransactionSource transactionSource;

				public Builder timestampSource(TimestampSource timestampSource) {
						this.timestampSource = timestampSource;
						return this;
				}

				public Builder dataLib(SDataLib<Data,
								Result, KeyValue, OperationWithAttributes,
								Put, Delete, Get, Scan, Lock, OperationStatus> dataLib) {
						this.dataLib = dataLib;
						return this;
				}

				public Builder dataWriter(
								STableWriter<Table, Mutation, Put, Delete, Data, Lock,
												org.apache.hadoop.hbase.regionserver.OperationStatus> dataWriter) {
						this.dataWriter = dataWriter;
						return this;
				}

				public Builder dataStore(DataStore<Data, Hashable,
								Result, KeyValue, OperationWithAttributes,
								Mutation, Put, Delete, Get, Scan, Table, Lock, OperationStatus, Scanner> dataStore) {
						this.dataStore = dataStore;
						return this;
				}

				public Builder transactionStore(TransactionStore transactionStore) {
						this.transactionStore = transactionStore;
						return this;
				}

				public Builder clock(Clock clock) {
						this.clock = clock;
						return this;
				}

				public Builder transactionTimeout(int transactionTimeoutMS) {
						this.transactionTimeoutMS = transactionTimeoutMS;
						return this;
				}

				public Builder hasher(Hasher<Data, Hashable> hasher) {
						this.hasher = hasher;
						return this;
				}

				public Builder transactionListener(TransactorListener listener) {
						this.listener = listener;
						return this;
				}

				public Builder transactionSource(TransactionSource transactionSource) {
						this.transactionSource = transactionSource;
						return this;
				}

				public SITransactor<Table,
								OperationWithAttributes,Mutation, Put,Get,
								Scan,Result,KeyValue,Data,Hashable,Delete,
								Lock,OperationStatus,Scanner> build(){
						return new SITransactor<Table,
										OperationWithAttributes,Mutation, Put,Get,Scan,
										Result,KeyValue,Data,Hashable,
										Delete,Lock,OperationStatus,Scanner>(timestampSource,dataLib,dataWriter,
										dataStore,transactionStore,clock,transactionTimeoutMS,hasher,listener);
				}

		}
}
