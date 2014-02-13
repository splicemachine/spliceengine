package com.splicemachine.si.impl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.*;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableWriter;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Central point of implementation of the "snapshot isolation" MVCC algorithm that provides transactions across atomic
 * row updates in the underlying store. This is the core brains of the SI logic.
 */
@SuppressWarnings("unchecked")

public class SITransactor<Table, OperationWithAttributes,
				Mutation extends OperationWithAttributes,
				Put extends Mutation,
				Get extends OperationWithAttributes,
        Scan extends OperationWithAttributes,
				Result, KeyValue, Data, Hashable extends Comparable,
        Delete extends OperationWithAttributes, Lock, OperationStatus, Scanner>
        implements Transactor<Table, Put, Mutation, OperationStatus, Data, Hashable> {
    static final Logger LOG = Logger.getLogger(SITransactor.class);
    public static final int MAX_ACTIVE_COUNT = 1000;

		private final SDataLib<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock, OperationStatus> dataLib;
    private final STableWriter<Table, Mutation, Put, Delete, Data, Lock, org.apache.hadoop.hbase.regionserver.OperationStatus> dataWriter;
    private final DataStore<Data, Hashable, Result, KeyValue, OperationWithAttributes, Mutation, Put, Delete, Get, Scan, Table, Lock, OperationStatus, Scanner> dataStore;
    private final TransactionStore transactionStore;
    private final Clock clock;
    private final int transactionTimeoutMS;
    private final Hasher<Data, Hashable> hasher;

		private final TransactionSource transactionSource;

		private final TransactionManager transactionManager;
		private final ClientTransactor<Put,Get,Scan,Mutation,Data> clientTransactor;

		private SITransactor(SDataLib dataLib,
												 STableWriter dataWriter,
												 DataStore dataStore,
												 final TransactionStore transactionStore,
												 Clock clock,
												 int transactionTimeoutMS,
												 Hasher<Data, Hashable> hasher,
												 ClientTransactor<Put, Get, Scan, Mutation, Data> clientTransactor,
												 TransactionManager transactionManager) {
				transactionSource = new TransactionSource() {
            @Override
            public Transaction getTransaction(long timestamp) throws IOException {
                return transactionStore.getTransaction(timestamp);
            }
        };
        this.dataLib = dataLib;
        this.dataWriter = dataWriter;
        this.dataStore = dataStore;
        this.transactionStore = transactionStore;
        this.clock = clock;
        this.transactionTimeoutMS = transactionTimeoutMS;
        this.hasher = hasher;
				this.clientTransactor = clientTransactor;
				this.transactionManager = transactionManager;
    }

		// Operation pre-processing. These are to be called "server-side" when we are about to process an operation.

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
        @SuppressWarnings("unchecked") final Mutation[] mutations = (Mutation[]) Array.newInstance(put.getClass(), 1);
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
				TransactionId txn = transactionManager.transactionIdFromString(txnId);
				ImmutableTransaction transaction = transactionStore.getImmutableTransaction(txn);
				ensureTransactionAllowsWrites(transaction);

				Pair<KVPair,Lock>[] lockPairs = null;
				try {
						lockPairs = lockRows(table, mutations);

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

		@SuppressWarnings("UnusedDeclaration")
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
						finalPuts[i] = Pair.newPair(getMutationToRun(rollForwardQueue, dataAndLocks[i].getFirst(), transaction, conflictResults),dataAndLocks[i].getSecond());
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

		private Pair<KVPair,Lock>[] lockRows(Table table, Collection<KVPair> mutations) throws IOException {
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


		private Mutation getMutationToRun(RollForwardQueue<Data, Hashable> rollForwardQueue, KVPair kvPair, ImmutableTransaction transaction, ConflictResults conflictResults) throws IOException{
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
				@SuppressWarnings("unchecked") Set<Long>[] conflicts = new Set[2];
        for (KeyValue dataCommitKeyValue : dataCommitKeyValues) {
            checkCommitTimestampForConflict(updateTransaction, conflicts, dataCommitKeyValue);
        }
        return new ConflictResults(conflicts[0], conflicts[1], null);
    }

    @SuppressWarnings("StatementWithEmptyBody")
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
            transactionManager.fail(dataTransaction.getTransactionId());
            checkTransactionTimeout(dataTransaction.getParent());
            return true;
        } else return !dataTransaction.isRootTransaction() && checkTransactionTimeout(dataTransaction.getParent());
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
        if (clientTransactor.isDeletePut(put)) {
            dataStore.setTombstonesOnColumns(table, transactionId, newPut);
        } else if (hasTombstone) {
            dataStore.setAntiTombstoneOnPut(newPut, transactionId);
        }
        return newPut;
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
                    if (transaction.getEffectiveStatus().isCommitted()) {
                        dataStore.setCommitTimestamp(table, row, transaction.getLongTransactionId(), transaction.getEffectiveCommitTimestamp());
                    }
										else {
                        dataStore.setCommitTimestampToFail(table, row, transaction.getLongTransactionId());
                    }
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


// Helpers

    /**
     * Is this operation supposed to be handled by "snapshot isolation".
     */
    private boolean isFlaggedForSITreatment(OperationWithAttributes operation) {
        return dataStore.getSINeededAttribute(operation) != null;
    }

		/**
     * Throw an exception if this is a read-only transaction.
     */
    private void ensureTransactionAllowsWrites(ImmutableTransaction transaction) throws IOException {
        if (transaction.isReadOnly()) {
            throw new DoNotRetryIOException("transaction is read only: " + transaction.getTransactionId().getTransactionIdString());
        }
    }

		public static class Builder<Data,
						Result,KeyValue,OperationWithAttributes,Mutation extends OperationWithAttributes,
						Put extends Mutation,Delete extends OperationWithAttributes,Get extends OperationWithAttributes,Scan extends OperationWithAttributes,
						Lock,OperationStatus,Table,Hashable extends Comparable<Hashable>,Scanner>{
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
				private TransactionManager control;
				private ClientTransactor<Put,Get,Scan,Mutation,Data> clientTransactor;

				public Builder control(TransactionManager control) {
						this.control = control;
						return this;
				}

				public Builder clientTransactor(ClientTransactor<Put, Get, Scan, Mutation, Data> clientTransactor) {
						this.clientTransactor = clientTransactor;
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

				public SITransactor<Table,
								OperationWithAttributes,Mutation, Put,Get,
								Scan,Result,KeyValue,Data,Hashable,Delete,
								Lock,OperationStatus,Scanner> build(){
						return new SITransactor<Table,
										OperationWithAttributes,Mutation, Put,Get,Scan,
										Result,KeyValue,Data,Hashable,
										Delete,Lock,OperationStatus,Scanner>(dataLib,dataWriter,
										dataStore,transactionStore,clock,transactionTimeoutMS,hasher,clientTransactor,control);
				}

		}
}
