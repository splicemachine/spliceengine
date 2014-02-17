package com.splicemachine.si.impl;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.*;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableWriter;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Central point of implementation of the "snapshot isolation" MVCC algorithm that provides transactions across atomic
 * row updates in the underlying store. This is the core brains of the SI logic.
 */
@SuppressWarnings("unchecked")

public class SITransactor<Table,
				Mutation extends OperationWithAttributes,
				Put extends Mutation,
				Get extends OperationWithAttributes,
        Scan extends OperationWithAttributes,
				Delete extends OperationWithAttributes>
        implements Transactor<Table, Mutation,Put> {
    static final Logger LOG = Logger.getLogger(SITransactor.class);
    public static final int MAX_ACTIVE_COUNT = 1000;

		private final SDataLib<Put, Delete, Get, Scan> dataLib;
    private final STableWriter<Table, Mutation, Put, Delete> dataWriter;
    private final DataStore<Mutation, Put, Delete, Get, Scan, Table> dataStore;
    private final TransactionStore transactionStore;
    private final Clock clock;
    private final int transactionTimeoutMS;

		private final TransactionSource transactionSource;

		private final TransactionManager transactionManager;

		private SITransactor(SDataLib dataLib,
												 STableWriter dataWriter,
												 DataStore dataStore,
												 final TransactionStore transactionStore,
												 Clock clock,
												 int transactionTimeoutMS,
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
				this.transactionManager = transactionManager;
    }

		// Operation pre-processing. These are to be called "server-side" when we are about to process an operation.

		// Process update operations

    @Override
    public boolean processPut(Table table, RollForwardQueue rollForwardQueue, Put put) throws IOException {
				if(!isFlaggedForSITreatment(put)) return false;

				final Put[] mutations = (Put[])Array.newInstance(put.getClass(),1);
				mutations[0] = put;
				OperationStatus[] operationStatuses = processPutBatch(table,rollForwardQueue,mutations);
				switch(operationStatuses[0].getOperationStatusCode()){
						case NOT_RUN:
								return false;
						case BAD_FAMILY:
								throw new NoSuchColumnFamilyException(operationStatuses[0].getExceptionMsg());
						case SANITY_CHECK_FAILURE:
								throw new IOException("Sanity Check failure:" +operationStatuses[0].getExceptionMsg());
						case FAILURE:
								throw new IOException(operationStatuses[0].getExceptionMsg());
						default:
								return true;
				}
    }

		@Override
    public OperationStatus[] processPutBatch(Table table, RollForwardQueue rollForwardQueue, Put[] mutations)
            throws IOException {
        if (mutations.length == 0) {
            //short-circuit special case of empty batch
						//noinspection unchecked
						return dataStore.writeBatch(table, new Pair[0]);
        }
				/*
				 * Here we convert a Put into a KVPair.
				 *
				 * Each Put represents a single row, but a KVPair represents a single column. Each row
				 * is written with a single transaction.
				 *
				 * What we do here is we group up the puts by their Transaction id (just in case they are different),
				 * then we group them up by family and column to create proper KVPair groups. Then, we attempt
				 * to write all the groups in sequence.
				 *
				 * Note the following:
				 *
				 * 1) We do all this as support for things that probably don't happen. With Splice's Packed Row
				 * Encoding, it is unlikely that people will send more than a single column of data over each
				 * time. Additionally, people likely won't send over a batch of Puts that have more than one
				 * transaction id (as that would be weird). Still, better safe than sorry.
				 *
				 * 2). This method is, because of all the regrouping and the partial writes and stuff,
				 * Significantly slower than the equivalent KVPair method, so It is highly recommended that you
				 * use the BulkWrite pipeline along with the KVPair abstraction to improve your overall throughput.
				 *
				 *
				 * To be frank, this is only here to support legacy code without needing to rewrite everything under
				 * the sun. You should almost certainly NOT use it.
				 */
				Map<String,Map<byte[],Map<byte[],List<KVPair>>>> kvPairMap = Maps.newHashMap();
				for(Put mutation:mutations){
						String txnId = dataStore.getTransactionid(mutation);
						boolean isDelete = dataStore.getDeletePutAttribute(mutation);
						Iterable<KeyValue> keyValues = dataLib.listPut(mutation);
						byte[] row = dataLib.getPutKey(mutation);
						for(KeyValue keyValue:keyValues){
								byte[] family = keyValue.getFamily();
								byte[] column = keyValue.getQualifier();
								byte[] value = keyValue.getValue();
								Map<byte[],Map<byte[],List<KVPair>>> familyMap = kvPairMap.get(txnId);
								if(familyMap==null){
										familyMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
										kvPairMap.put(txnId,familyMap);
								}
								Map<byte[],List<KVPair>> columnMap = familyMap.get(family);
								if(columnMap==null){
										columnMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
										familyMap.put(family,columnMap);
								}
								List<KVPair> kvPairs = columnMap.get(column);
								if(kvPairs==null){
										kvPairs = Lists.newArrayList();
										columnMap.put(column,kvPairs);
								}
								kvPairs.add(new KVPair(row,value,isDelete? KVPair.Type.DELETE : KVPair.Type.INSERT));
						}
				}
				final Map<byte[],OperationStatus> statusMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
				for(Map.Entry<String,Map<byte[],Map<byte[],List<KVPair>>>> entry:kvPairMap.entrySet()){
						String txnId = entry.getKey();
						Map<byte[],Map<byte[],List<KVPair>>> familyMap = entry.getValue();
						for(Map.Entry<byte[],Map<byte[],List<KVPair>>> familyEntry:familyMap.entrySet()){
								byte[] family = familyEntry.getKey();
								Map<byte[],List<KVPair>> columnMap = familyEntry.getValue();
								for(Map.Entry<byte[],List<KVPair>>columnEntry:columnMap.entrySet()){
										byte[] qualifier = columnEntry.getKey();
										List<KVPair> kvPairs = Lists.newArrayList(Collections2.filter(columnEntry.getValue(), new Predicate<KVPair>() {
												@Override
												public boolean apply(@Nullable KVPair input) {
														assert input != null;
														return !statusMap.containsKey(input.getRow()) || statusMap.get(input.getRow()).getOperationStatusCode() == HConstants.OperationStatusCode.SUCCESS;
												}
										}));
										OperationStatus[] statuses = processKvBatch(table,rollForwardQueue,family,qualifier,kvPairs,txnId);
										for(int i=0;i<statuses.length;i++){
												byte[] row = kvPairs.get(i).getRow();
												OperationStatus status = statuses[i];
												if(statusMap.containsKey(row)){
														OperationStatus oldStatus = statusMap.get(row);
														status = getCorrectStatus(status,oldStatus);
												}
												statusMap.put(row,status);
										}
								}
						}
				}
				OperationStatus[] retStatuses = new OperationStatus[mutations.length];
				for(int i=0;i<mutations.length;i++){
						Put put = mutations[i];
						retStatuses[i] = statusMap.get(dataLib.getPutKey(put));
				}
				return retStatuses;
    }

		private OperationStatus getCorrectStatus(OperationStatus status, OperationStatus oldStatus) {
				switch(oldStatus.getOperationStatusCode()){
						case SUCCESS:
								return status;
						case NOT_RUN:
						case BAD_FAMILY:
						case SANITY_CHECK_FAILURE:
						case FAILURE:
								return oldStatus;
				}
				return null;
		}

		@Override
		public OperationStatus[] processKvBatch(Table table,
																						RollForwardQueue rollForwardQueue,
																						TransactionId txnId,
																						byte[] family, byte[] qualifier,
																						Collection<KVPair> mutations) throws IOException {
				ImmutableTransaction transaction = transactionStore.getImmutableTransaction(txnId);
				ensureTransactionAllowsWrites(transaction);

				Pair<KVPair,Integer>[] lockPairs = null;
				try {
						lockPairs = lockRows(table, mutations);

						@SuppressWarnings("unchecked") final Set<Long>[] conflictingChildren = new Set[mutations.size()];
						dataStore.startLowLevelOperation(table);
						Pair<Mutation,Integer>[] writes;
						try {
								writes = checkConflictsForKvBatch(table, rollForwardQueue, lockPairs,
												conflictingChildren, transaction,family,qualifier);
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

		@Override
		public OperationStatus[] processKvBatch(Table table, RollForwardQueue rollForwardQueue,
																						byte[] family, byte[] qualifier,
																						Collection<KVPair> mutations, String txnId) throws IOException {
				if(mutations.size()<=0)
						//noinspection unchecked
						return dataStore.writeBatch(table,new Pair[]{});

				//ensure the transaction is in good shape
				TransactionId txn = transactionManager.transactionIdFromString(txnId);
				return processKvBatch(table,rollForwardQueue,txn,family,qualifier,mutations);
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

		private void releaseLocksForKvBatch(Table table, Pair<KVPair, Integer>[] locks){
				if(locks==null) return;
				for(Pair<KVPair,Integer>lock:locks){
						try {
								dataWriter.unLockRow(table,lock.getSecond());
						} catch (IOException e) {
								LOG.error("Unable to unlock row "+ Bytes.toStringBinary(lock.getFirst().getRow()),e);
								//ignore otherwise
						}
				}
		}

		private void resolveConflictsForKvBatch(Table table, Pair<Mutation, Integer>[] mutations, Set<Long>[] conflictingChildren, OperationStatus[] status){
				for(int i=0;i<mutations.length;i++){
						try{
								Put put = (Put)mutations[i].getFirst();
								Integer lock = mutations[i].getSecond();
								resolveChildConflicts(table,put,lock,conflictingChildren[i]);
						}catch(Exception ex){
								status[i]= new OperationStatus(HConstants.OperationStatusCode.FAILURE, ex.getMessage());
						}
				}
		}

		private Pair<Mutation, Integer>[] checkConflictsForKvBatch(Table table, RollForwardQueue rollForwardQueue,
																															 Pair<KVPair, Integer>[] dataAndLocks,
																															 Set<Long>[] conflictingChildren,
																															 ImmutableTransaction transaction, byte[] family, byte[] qualifier) throws IOException{
				@SuppressWarnings("unchecked") Pair<Mutation,Integer>[] finalPuts = new Pair[dataAndLocks.length];
				for(int i=0;i<dataAndLocks.length;i++){
						ConflictResults conflictResults;
						if(unsafeWrites)
							conflictResults = ConflictResults.NO_CONFLICT;
						else{
								List<KeyValue>[] possibleConflicts = dataStore.getCommitTimestampsAndTombstonesSingle(table, dataAndLocks[i].getFirst().getRow());
								conflictResults = ensureNoWriteConflict(transaction,possibleConflicts);
						}
						conflictingChildren[i] = conflictResults.childConflicts;
						finalPuts[i] = Pair.newPair(getMutationToRun(rollForwardQueue,
										dataAndLocks[i].getFirst(), family, qualifier,transaction, conflictResults),dataAndLocks[i].getSecond());
				}
				return finalPuts;
		}

		private static final boolean unsafeWrites = Boolean.getBoolean("splice.unsafe.writes");


		private Pair<KVPair, Integer>[] lockRows(Table table, Collection<KVPair> mutations) throws IOException {
				@SuppressWarnings("unchecked") Pair<KVPair,Integer>[] mutationsAndLocks = new Pair[mutations.size()];

				int i=0;
				for(KVPair pair:mutations){
						mutationsAndLocks[i] = Pair.newPair(pair,dataWriter.lockRow(table, pair.getRow()));
						i++;
				}
				return mutationsAndLocks;
		}


		private Mutation getMutationToRun(RollForwardQueue rollForwardQueue, KVPair kvPair,
																			byte[] family, byte[] column,
																			ImmutableTransaction transaction, ConflictResults conflictResults) throws IOException{
				long txnIdLong = transaction.getLongTransactionId();
				Put newPut = dataLib.toPut(kvPair,family, column,transaction.getLongTransactionId());
				dataStore.suppressIndexing(newPut);
				dataStore.addTransactionIdToPutKeyValues(newPut, txnIdLong);
				if(kvPair.getType()== KVPair.Type.DELETE)
						dataStore.setTombstoneOnPut(newPut,txnIdLong);
				else if(conflictResults.hasTombstone)
						dataStore.setAntiTombstoneOnPut(newPut, txnIdLong);

				byte[]row = kvPair.getRow();
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

		private void resolveChildConflicts(Table table, Put put, Integer lock, Set<Long> conflictingChildren) throws IOException {
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
                if (tombstone.getTimestamp() == transactionId) {
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
        final long dataTransactionId = dataCommitKeyValue.getTimestamp();
        if (!updateTransaction.sameTransaction(dataTransactionId)) {
            final byte[] commitTimestampValue = dataCommitKeyValue.getValue();
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
                final long dataCommitTimestamp = dataLib.decode(commitTimestampValue, Long.class);
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


		// Roll-forward / compaction

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

		public static class Builder<
						Mutation extends OperationWithAttributes,
						Put extends Mutation,Delete extends OperationWithAttributes,Get extends OperationWithAttributes,Scan extends OperationWithAttributes,
						Table>{
				private SDataLib<
								Put,  Delete,  Get,  Scan
								> dataLib;
				private STableWriter<Table, Mutation, Put, Delete> dataWriter;
				private DataStore<
								Mutation, Put, Delete, Get, Scan,
								Table> dataStore;
				private TransactionStore transactionStore;
				private Clock clock;
				private int transactionTimeoutMS;
				private TransactionManager control;

				public Builder() {
				}

				public Builder control(TransactionManager control) {
						this.control = control;
						return this;
				}

				public Builder dataLib(SDataLib<
								Put, Delete, Get, Scan> dataLib) {
						this.dataLib = dataLib;
						return this;
				}

				public Builder dataWriter(
								STableWriter<Table, Mutation, Put, Delete
												> dataWriter) {
						this.dataWriter = dataWriter;
						return this;
				}

				public Builder dataStore(DataStore<
								Mutation, Put, Delete, Get, Scan, Table> dataStore) {
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

				public SITransactor<Table,
								Mutation, Put,Get,
								Scan, Delete> build(){
						return new SITransactor<Table,
										Mutation, Put,Get,Scan,
										Delete> (dataLib,dataWriter,
										dataStore,transactionStore,clock,transactionTimeoutMS, control);
				}

		}
}
