package com.splicemachine.si.impl;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.*;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableWriter;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
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
		/*Singleton field to save memory when we are unable to acquire locks*/
		private static final OperationStatus NOT_RUN = new OperationStatus(HConstants.OperationStatusCode.NOT_RUN);

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
						byte[] row = dataLib.getPutKey(mutation);
						Iterable<KeyValue> keyValues = dataLib.listPut(mutation);
						boolean isSIDataOnly = true;
						for(KeyValue keyValue:keyValues){
								byte[] family = keyValue.getFamily();
								byte[] column = keyValue.getQualifier();
								if(!Bytes.equals(column,SIConstants.PACKED_COLUMN_BYTES)) {
										continue; //skip SI columns
								}

								isSIDataOnly = false;
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
						if(isSIDataOnly){
							/*
							 * Someone attempted to write only SI data, which means that the values column is empty.
							 * Put a KVPair which is an empty byte[] for all the columns in the data
							 */
								byte[] family = SpliceConstants.DEFAULT_FAMILY_BYTES;
								byte[] column = SpliceConstants.PACKED_COLUMN_BYTES;
								byte[] value = new byte[]{};
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
								kvPairs.add(new KVPair(row,value,isDelete? KVPair.Type.DELETE : KVPair.Type.EMPTY_COLUMN));
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
			return processKvBatch(table,rollForwardQueue,txnId,family,qualifier,mutations,null);
		}

		@Override
		public OperationStatus[] processKvBatch(Table table,
																						RollForwardQueue rollForwardQueue,
																						TransactionId txnId,
																						byte[] family, byte[] qualifier,
																						Collection<KVPair> mutations,ConstraintChecker constraintChecker) throws IOException {
				ImmutableTransaction transaction = transactionStore.getImmutableTransaction(txnId);
				ensureTransactionAllowsWrites(transaction);

				OperationStatus[] finalStatus = new OperationStatus[mutations.size()];
				Pair<KVPair,Integer>[] lockPairs = new Pair[mutations.size()];
				FilterState constraintState = null;
				if(constraintChecker!=null)
						constraintState = new FilterState(dataLib,dataStore,transactionStore,rollForwardQueue,transaction);
				try {
						lockRows(table,mutations,lockPairs,finalStatus);

						@SuppressWarnings("unchecked") final Set<Long>[] conflictingChildren = new Set[mutations.size()];
						dataStore.startLowLevelOperation(table);
						IntObjectOpenHashMap<Pair<Mutation,Integer>> writes;
						try {
								writes = checkConflictsForKvBatch(table, rollForwardQueue, lockPairs,
												conflictingChildren, transaction,family,qualifier,constraintChecker,constraintState,finalStatus);
						} finally {
								dataStore.closeLowLevelOperation(table);
						}

						//TODO -sf- this can probably be made more efficient
						//convert into array for usefulness
						Pair<Mutation,Integer>[] toWrite = new Pair[writes.size()];
						int i=0;
						for(IntObjectCursor<Pair<Mutation,Integer>> write:writes){
								toWrite[i] = write.value;
								i++;
						}
						final OperationStatus[] status = dataStore.writeBatch(table, toWrite);

						resolveConflictsForKvBatch(table, toWrite, conflictingChildren, status);

						//convert the status back into the larger array
						i=0;
						for(IntObjectCursor<Pair<Mutation,Integer>> write:writes){
								finalStatus[write.key] = status[i];
								i++;
						}

						return finalStatus;
				} finally {
						releaseLocksForKvBatch(table, lockPairs);
				}
		}

		@Override
		public OperationStatus[] processKvBatch(Table table, RollForwardQueue rollForwardQueue,
																						byte[] family, byte[] qualifier,
																						Collection<KVPair> mutations,
																						String txnId) throws IOException {
			return processKvBatch(table,rollForwardQueue,family,qualifier,mutations,txnId,null);
		}

		@Override
		public OperationStatus[] processKvBatch(Table table,
																						RollForwardQueue rollForwardQueue,
																						byte[] family,
																						byte[] qualifier,
																						Collection<KVPair> mutations,
																						String txnId, ConstraintChecker constraintChecker) throws IOException {
				if(mutations.size()<=0)
						//noinspection unchecked
						return dataStore.writeBatch(table,new Pair[]{});

				//ensure the transaction is in good shape
				TransactionId txn = transactionManager.transactionIdFromString(txnId);
				return processKvBatch(table,rollForwardQueue,txn,family,qualifier,mutations,constraintChecker);
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
						if(lock==null) continue;
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
							ex.printStackTrace();
								status[i]= new OperationStatus(HConstants.OperationStatusCode.FAILURE, ex.getMessage());
						}
				}
		}

		private IntObjectOpenHashMap<Pair<Mutation, Integer>> checkConflictsForKvBatch(Table table,
																																									 RollForwardQueue rollForwardQueue,
																															 Pair<KVPair, Integer>[] dataAndLocks,
																															 Set<Long>[] conflictingChildren,
																															 ImmutableTransaction transaction,
																															 byte[] family, byte[] qualifier,
																															 ConstraintChecker constraintChecker,
																															 FilterState constraintStateFilter,
																															 OperationStatus[] finalStatus) throws IOException{
				IntObjectOpenHashMap<Pair<Mutation,Integer>> finalMutationsToWrite = IntObjectOpenHashMap.newInstance();
				for(int i=0;i<dataAndLocks.length;i++){
						Pair<KVPair,Integer> baseDataAndLock = dataAndLocks[i];
						if(baseDataAndLock==null) continue;

						ConflictResults conflictResults = ConflictResults.NO_CONFLICT;
						KVPair kvPair = baseDataAndLock.getFirst();
						if(unsafeWrites && constraintChecker!=null){
								//still have to check the constraint
								Result row = dataStore.getCommitTimestampsAndTombstonesSingle(table, kvPair.getRow());
								if (applyConstraint(constraintChecker,constraintStateFilter, i, kvPair,row,finalStatus))
										continue;
						} else{
								Result possibleConflicts = dataStore.getCommitTimestampsAndTombstonesSingle(table, kvPair.getRow());
								if (possibleConflicts != null) {
										conflictResults = ensureNoWriteConflict(transaction,possibleConflicts);
										//if we've got this far, we have no write conflicts
										if (applyConstraint(constraintChecker,constraintStateFilter, i, kvPair, possibleConflicts, finalStatus))
												continue;
								}
						}
						conflictingChildren[i] = conflictResults.childConflicts;
						Mutation mutationToRun = getMutationToRun(table, rollForwardQueue, kvPair,
																											family, qualifier, transaction, conflictResults);
						finalMutationsToWrite.put(i, Pair.newPair(mutationToRun, baseDataAndLock.getSecond()));
				}
				return finalMutationsToWrite;
		}

		private boolean applyConstraint(ConstraintChecker constraintChecker,
																		FilterState constraintStateFilter,
																		int rowPosition,
																		KVPair mutation, Result row,
																		OperationStatus[] finalStatus) throws IOException {
				/*
				 * Attempts to apply the constraint (if there is any). When this method returns true, the row should be filtered
				 * out.
				 *
				 */
				if(constraintChecker==null) return false;
				if(row==null || row.size()<=0) return false; //you can't apply a constraint on a non-existent row

				//we need to make sure that this row is visible to the current transaction
				for(KeyValue kv: row.raw()){
						Filter.ReturnCode code = constraintStateFilter.filterKeyValue(kv);
						switch(code){
								case NEXT_COL:
								case NEXT_ROW:
								case SEEK_NEXT_USING_HINT:
										return false; //row is not visible
						}
				}

				OperationStatus operationStatus = constraintChecker.checkConstraint(mutation, row);
				if(operationStatus!=null && operationStatus.getOperationStatusCode()== HConstants.OperationStatusCode.FAILURE){
						finalStatus[rowPosition] = operationStatus;
						return true;
				}
				return false;
		}

		private static final boolean unsafeWrites = Boolean.getBoolean("splice.unsafe.writes");


		private void lockRows(Table table, Collection<KVPair> mutations, Pair<KVPair,Integer>[] mutationsAndLocks,OperationStatus[] finalStatus){
				/*
				 * We attempt to lock each row in the collection.
				 *
				 * If the lock is acquired, we place it into mutationsAndLocks (at the position equal
				 * to the position in the collection's iterator).
				 *
				 * If the lock cannot be acquired, then we set NOT_RUN into the finalStatus array. Those rows will be filtered
				 * out and must be retried by the writer. mutationsAndLocks at the same location will be null
				 */
				int position=0;
				for(KVPair mutation:mutations){
						Integer lock = dataWriter.tryLock(table,mutation.getRow());
						if(lock!=null)
								mutationsAndLocks[position] = Pair.newPair(mutation,lock);
						else
							finalStatus[position] = NOT_RUN;

						position++;
				}
		}


		private Mutation getMutationToRun(Table table,RollForwardQueue rollForwardQueue, KVPair kvPair,
																			byte[] family, byte[] column,
																			ImmutableTransaction transaction, ConflictResults conflictResults) throws IOException{
				long txnIdLong = transaction.getLongTransactionId();
				Put newPut;
				if(kvPair.getType() == KVPair.Type.EMPTY_COLUMN){
						/*
						 * WARNING: This requires a read of column data to populate! Try not to use
						 * it unless no other option presents itself.
						 *
						 * In point of fact, this only occurs if someone sends over a non-delete Put
						 * which has only SI data. In the event that we send over a row with all nulls
						 * from actual Splice system, we end up with a KVPair that has a non-empty byte[]
						 * for the values column (but which is nulls everywhere)
						 */
						newPut = dataLib.newPut(kvPair.getRow());
						dataStore.setTombstonesOnColumns(table,txnIdLong,newPut);
				}else if(kvPair.getType()== KVPair.Type.DELETE){
						newPut = dataLib.newPut(kvPair.getRow());
						dataStore.setTombstoneOnPut(newPut,txnIdLong);
				}else
						newPut = dataLib.toPut(kvPair,family, column,transaction.getLongTransactionId());

				dataStore.suppressIndexing(newPut);
				if(kvPair.getType()!= KVPair.Type.DELETE && conflictResults.hasTombstone)
						dataStore.setAntiTombstoneOnPut(newPut, txnIdLong);

				byte[]row = kvPair.getRow();
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
    private ConflictResults ensureNoWriteConflict(ImmutableTransaction updateTransaction, Result result) throws IOException {
    	// XXX TODO jleach: Create a filter to determine this conflict, no reason to materialize a lot of data across the wire.
    	final ConflictResults timestampConflicts = checkTimestampsHandleNull(updateTransaction,result.getColumnLatest(
    			SIConstants.DEFAULT_FAMILY_BYTES, SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES),
    			result.getColumnLatest(SIConstants.DEFAULT_FAMILY_BYTES, SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES),
    			result.getColumnLatest(SIConstants.DEFAULT_FAMILY_BYTES, SIConstants.PACKED_COLUMN_BYTES)
    			);	
        boolean hasTombstone = hasCurrentTransactionTombstone(updateTransaction.getLongTransactionId(),
        		result.getColumnLatest(SIConstants.DEFAULT_FAMILY_BYTES, SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES));        
        return new ConflictResults(timestampConflicts.toRollForward, timestampConflicts.childConflicts, hasTombstone);
    }

    private boolean hasCurrentTransactionTombstone(long transactionId, KeyValue tombstoneValue) {
				return tombstoneValue != null && tombstoneValue.getTimestamp() == transactionId && !dataStore.isAntiTombstone(tombstoneValue);
		}


		private ConflictResults checkTimestampsHandleNull(ImmutableTransaction updateTransaction, KeyValue dataCommitKeyValue, KeyValue tombstoneKeyValue, KeyValue dataKeyValue) throws IOException {
        if (dataCommitKeyValue == null && dataKeyValue == null && tombstoneKeyValue == null) {
            return new ConflictResults(Collections.<Long>emptySet(),Collections.<Long>emptySet(), null);
        } else {
            return checkCommitTimestampsForConflicts(updateTransaction, dataCommitKeyValue, tombstoneKeyValue, dataKeyValue);
        }
    }

    /**
		 * Look at all of the values in the "commitTimestamp" column to see if there are write collisions.
		 */
		private ConflictResults checkCommitTimestampsForConflicts(ImmutableTransaction updateTransaction, KeyValue dataCommitKeyValue, KeyValue tombstoneKeyValue, KeyValue dataKeyValue)
						throws IOException {
				@SuppressWarnings("unchecked") Set<Long>[] conflicts = new Set[2]; // auto boxing XXX TODO Jleach
				if (dataCommitKeyValue != null) {
						checkCommitTimestampForConflict(updateTransaction, conflicts, dataCommitKeyValue);
				}
				if (tombstoneKeyValue != null) {
						checkDataForConflict(updateTransaction, conflicts, tombstoneKeyValue);
				}
				if (dataKeyValue != null) {
						checkDataForConflict(updateTransaction, conflicts, dataKeyValue);
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

	private void checkDataForConflict(ImmutableTransaction updateTransaction,Set<Long>[] conflicts, KeyValue dataCommitKeyValue)
            throws IOException {
        final long dataTransactionId = dataCommitKeyValue.getTimestamp();
        if (!updateTransaction.sameTransaction(dataTransactionId)) {
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
