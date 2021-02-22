/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.si.impl.server;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.splicemachine.access.api.Durability;
import com.splicemachine.access.configuration.SIConfigurations;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.*;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.rollforward.RollForward;
import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.api.txn.ConflictType;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.api.txn.WriteConflict;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.ConflictResults;
import com.splicemachine.si.impl.SimpleTxnFilter;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.store.ActiveTxnCacheSupplier;
import com.splicemachine.storage.*;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import splice.com.google.common.collect.Collections2;
import splice.com.google.common.collect.Lists;
import splice.com.google.common.collect.Maps;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;

/**
 * Central point of implementation of the "snapshot isolation" MVCC algorithm that provides transactions across atomic
 * row updates in the underlying store. This is the core brains of the SI logic.
 */
@SuppressWarnings("unchecked")
public class SITransactor implements Transactor{
    private static final Logger LOG=Logger.getLogger(SITransactor.class);
    private final OperationFactory opFactory;
    private final OperationStatusFactory operationStatusLib;
    private final ExceptionFactory exceptionLib;
    private ConstraintChecker defaultConstraintChecker;

    private final TxnOperationFactory txnOperationFactory;
    private final TxnSupplier txnSupplier;

    public class MutationToRun implements Transactor.MutationsToRun {
        int index;
        LongHashSet conflictingChildren;
        DataPut data;

        @Override
        public int getOriginalIndex() {
            return index;
        }

        @Override
        public DataPut getData() {
            return data;
        }
    }

    public SITransactor(TxnSupplier txnSupplier,
                        TxnOperationFactory txnOperationFactory,
                        OperationFactory opFactory,
                        OperationStatusFactory operationStatusLib,
                        ExceptionFactory exceptionFactory){
        this.txnSupplier=txnSupplier;
        this.txnOperationFactory=txnOperationFactory;
        this.opFactory= opFactory;
        this.operationStatusLib = operationStatusLib;
        this.exceptionLib = exceptionFactory;
        this.defaultConstraintChecker = operationStatusLib.getNoOpConstraintChecker();
    }

    // Operation pre-processing. These are to be called "server-side" when we are about to process an operation.

    // Process update operations

    @Override
    public boolean processPut(Partition table,RollForward rollForwardQueue,DataPut put) throws IOException{
        if(!isFlaggedForSITreatment(put)) return false;
        final DataPut[] mutations= {put};
        MutationStatus[] operationStatuses=processPutBatch(table,rollForwardQueue,mutations);
        return operationStatusLib.processPutStatus(operationStatuses[0]);
    }

    @Override
    public MutationStatus[] processPutBatch(Partition table,RollForward rollForwardQueue,
                                            DataPut[] mutations) throws IOException{
        if(mutations.length==0){
            //short-circuit special case of empty batch
            //noinspection unchecked
            return new MutationStatus[0];
        }

        // todo: this somehow looks improvable. do we need all these maps?

        Map<TxnView, Map<byte[], Map<byte[], List<KVPair>>>> kvPairMap =
                SITransactorUtil.putToKvPairMap(mutations, txnOperationFactory);

        final Map<byte[], MutationStatus> statusMap= Maps.newTreeMap(Bytes.BASE_COMPARATOR);
        for(Map.Entry<TxnView, Map<byte[], Map<byte[], List<KVPair>>>> entry : kvPairMap.entrySet()){
            TxnView txnId=entry.getKey();
            Map<byte[], Map<byte[], List<KVPair>>> familyMap=entry.getValue();
            for(Map.Entry<byte[], Map<byte[], List<KVPair>>> familyEntry : familyMap.entrySet()){
                byte[] family=familyEntry.getKey();
                Map<byte[], List<KVPair>> columnMap=familyEntry.getValue();
                for(Map.Entry<byte[], List<KVPair>> columnEntry : columnMap.entrySet()){
                    byte[] qualifier=columnEntry.getKey();
                    List<KVPair> kvPairs= Lists.newArrayList(Collections2.filter(columnEntry.getValue(), input -> {
                        assert input != null;
                        return !statusMap.containsKey(input.getRowKey()) || statusMap.get(input.getRowKey()).isSuccess();
                    }));
                    MutationStatus[] statuses = processKvBatch(table, null, family, qualifier, kvPairs,
                            txnId, defaultConstraintChecker);
                    for (int i = 0; i < statuses.length; i++) {
                        byte[] row = kvPairs.get(i).getRowKey();
                        MutationStatus status = statuses[i];
                        if (statusMap.containsKey(row)) {
                            MutationStatus oldStatus = statusMap.get(row);
                            // if status == SUCCESS, use status, otherwise oldStatus. or sometimes null.
                            status = getCorrectStatus(status, oldStatus);
                        }
                        statusMap.put(row, status);
                    }
                }
            }
        }
        MutationStatus[] retStatuses=new MutationStatus[mutations.length];
        for(int i=0;i<mutations.length;i++){
            DataPut put=mutations[i];
            retStatuses[i]=statusMap.get(put.key());
        }
        return retStatuses;
    }



    @Override
    public MutationStatus[] processKvBatch(Partition table,
                                           RollForward rollForward,
                                           byte[] defaultFamilyBytes,
                                           byte[] packedColumnBytes,
                                           Collection<KVPair> toProcess,
                                           TxnView txn,
                                           ConstraintChecker constraintChecker) throws IOException{
        return processKvBatch(table,rollForward,defaultFamilyBytes,packedColumnBytes,toProcess,txn,constraintChecker, false, false, false);
    }

    @Override
    public MutationStatus[] processKvBatch(Partition table,
                                           RollForward rollForward,
                                           byte[] defaultFamilyBytes,
                                           byte[] packedColumnBytes,
                                           Collection<KVPair> toProcess,
                                           TxnView txn,
                                           ConstraintChecker constraintChecker,
                                           boolean skipConflictDetection,
                                           boolean skipWAL, boolean rollforward) throws IOException{
        ensureTransactionAllowsWrites(txn);
        return processInternal(table,rollForward,txn,defaultFamilyBytes,packedColumnBytes,toProcess,constraintChecker,skipConflictDetection,skipWAL,rollforward);
    }

    @Override
    public void setDefaultConstraintChecker(ConstraintChecker constraintChecker) {
        this.defaultConstraintChecker = constraintChecker;
    }

    private MutationStatus getCorrectStatus(MutationStatus status,MutationStatus oldStatus){
        return operationStatusLib.getCorrectStatus(status,oldStatus);
    }

    class TemporaryWriteState implements Transactor.TemporaryWriteState {
        ArrayList<MutationToRun> mutationsToRun;
        Partition table;
        ConflictRollForward conflictRollForward;
        Pair<KVPair, Lock>[] lockPairs;
        MutationStatus[] finalStatus;

        boolean cleanedUp = false;


        @Override
        public MutationStatus[] prepare(Partition table,
                                                RollForward rollForwardQueue,
                                                TxnView txn,
                                                byte[] family, byte[] qualifier,
                                                Collection<KVPair> mutations,
                                                ConstraintChecker constraintChecker,
                                                boolean skipConflictDetection,
                                                boolean skipWAL, boolean rollforward) throws IOException
        {
            this.table = table;
            int size = mutations.size();
            this.finalStatus=new MutationStatus[size];
            this.lockPairs=new Pair[size];

            SimpleTxnFilter constraintState=null;
            TxnSupplier supplier = null;
            if(constraintChecker!=null) {
                constraintState = new SimpleTxnFilter(null, txn, NoOpReadResolver.INSTANCE, txnSupplier);
                supplier = constraintState.getTxnSupplier();
            } else {
                SIDriver driver = SIDriver.driver();
                int initialSize = driver != null ?
                        driver.getConfiguration().getActiveTransactionInitialCacheSize() :
                        SIConfigurations.DEFAULT_ACTIVE_TRANSACTION_INITIAL_CACHE_SIZE;
                int maxSize = driver != null ?
                        driver.getConfiguration().getActiveTransactionMaxCacheSize() :
                        SIConfigurations.DEFAULT_ACTIVE_TRANSACTION_MAX_CACHE_SIZE;
                supplier = new ActiveTxnCacheSupplier(txnSupplier, initialSize, maxSize);
            }
            // todo: maybe checkConflictsForKvBatch should return a ArrayList of objects with
            // int index
            // LongHashSet conflictingChildren
            // DataPut data
            //@SuppressWarnings("unchecked") final LongHashSet[] conflictingChildren=new LongHashSet[size];

            this.conflictRollForward = new ConflictRollForward(opFactory, supplier);

            try {
                lockRows(table, mutations, lockPairs, finalStatus);

                /*
                 * You don't need a low-level operation check here, because this code can only be called from
                 * 1 of 2 paths (bulk write pipeline and SIObserver). Since both of those will externally ensure that
                 * the region can't close until after this method is complete, we don't need the calls.
                 */
                //IntObjectHashMap<DataPut> writes
                mutationsToRun = checkConflictsForKvBatch(table, conflictRollForward, lockPairs,
                        txn, family, qualifier, constraintChecker, constraintState, finalStatus,
                        skipConflictDetection, skipWAL, supplier, rollforward);
                return finalStatus;
            } finally {
                // todo: need to check here
            }
        }

        @Override
        public MutationStatus[] finishWrite(Set<KVPair> cantRunSet) throws IOException {
            DataPut[] toWrite = mutationsToRun.stream().map( c -> c.data ).
                    filter( d -> !cantRunSet.contains(d) ).toArray(DataPut[]::new);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Writing " + Arrays.toString(toWrite));
            }
            Iterator<MutationStatus> status = table.writeBatch(toWrite);

            //convert the status back into the larger array
            for(MutationToRun mutationToRun : mutationsToRun) {
                if(!status.hasNext())
                    throw new IllegalStateException("Programmer Error: incorrect length for returned status");
                finalStatus[mutationToRun.index] = status.next().getClone(); //TODO -sf- is clone needed here? //todo
                //resolve child conflicts
                try{
                    // todo: can we also get conflicts here with other writes to index tables?
                    resolveChildConflicts(table, mutationToRun.data, mutationToRun.conflictingChildren);
                }catch(Exception e){
                    finalStatus[mutationToRun.index] = operationStatusLib.failure(e);
                }
            }
            return finalStatus;
        }

        @Override
        public void cleanup() {
            if( cleanedUp ) return;
            cleanedUp = true;
            try {
                if(conflictRollForward != null) {
                    List<DataMutation> rollForwardMutations = conflictRollForward.getMutations();
                    if (rollForwardMutations.size() > 0)
                        table.batchMutate(rollForwardMutations);
                }
            } catch (Throwable t) {
                LOG.warn("Exception while trying to roll forward after conflict detection", t);
            }
            releaseLocksForKvBatch(lockPairs);
        }
    }

    public Transactor.TemporaryWriteState getTemporaryWriteState() {
        return new TemporaryWriteState();
    }


    private MutationStatus[] processInternal(Partition table,
                                             RollForward rollForwardQueue,
                                             TxnView txn,
                                             byte[] family, byte[] qualifier,
                                             Collection<KVPair> mutations,
                                             ConstraintChecker constraintChecker,
                                             boolean skipConflictDetection,
                                             boolean skipWAL, boolean rollforward) throws IOException{

        Transactor.TemporaryWriteState state = getTemporaryWriteState();

        try {
            state.prepare(table, rollForwardQueue, txn, family, qualifier,
                    mutations, constraintChecker, skipConflictDetection, skipWAL, rollforward);
            return state.finishWrite(new HashSet<>());
        } finally {
            state.cleanup();
        }
    }

    private void releaseLocksForKvBatch(Pair<KVPair, Lock>[] locks){
        if(locks==null) return;
        for(Pair<KVPair, Lock> lock : locks){
            if(lock==null || lock.getSecond()==null) continue;
            lock.getSecond().unlock();
        }
    }

    // todo: maybe just return an ArrayList<DataPut> with null values where we don't need stuff.
    private ArrayList<MutationToRun> checkConflictsForKvBatch(Partition table,
                                                              ConflictRollForward rollForwardQueue,
                                                              Pair<KVPair, Lock>[] dataAndLocks,
                                                              TxnView transaction,
                                                              byte[] family, byte[] qualifier,
                                                              ConstraintChecker constraintChecker,
                                                              TxnFilter constraintStateFilter,
                                                              MutationStatus[] finalStatus, boolean skipConflictDetection,
                                                              boolean skipWAL, TxnSupplier supplier, boolean rollforward) throws IOException {

        ArrayList<MutationToRun> mutationsToRun = new ArrayList<>(dataAndLocks.length);
        DataResult possibleConflicts = null;
        BitSet bloomInMemoryCheck  = skipConflictDetection ? null : table.getBloomInMemoryCheck(constraintChecker!=null,dataAndLocks);
        List<ByteSlice> toRollforward = null;
        if (rollforward) {
            toRollforward = new ArrayList<>(dataAndLocks.length);
        }
        for(int i = 0; i<dataAndLocks.length; i++) {
            Pair<KVPair, Lock> baseDataAndLock = dataAndLocks[i];
            if(baseDataAndLock==null) continue;

            ConflictResults conflictResults = ConflictResults.NO_CONFLICT;
            KVPair kvPair = baseDataAndLock.getFirst();
            KVPair.Type writeType = kvPair.getType();
            boolean checkedConflicts = false;
            if (!skipConflictDetection && (constraintChecker != null || !KVPair.Type.INSERT.equals(writeType))) {
                /*
                 *
                 * If the table has no keys, then the hbase row key is a randomly generated UUID, so it's not
                 * going to incur a write/write penalty, because there isn't any other row there (as long as we are inserting).
                 * Therefore, we do not need to perform a write/write conflict check or a constraint check
                 *
                 * We know that this is the case because there is no constraint checker (constraint checkers are only
                 * applied on key elements.
                 */
                //todo -sf remove the Row key copy here
                checkedConflicts = true;
                if( bloomInMemoryCheck != null && bloomInMemoryCheck.get(i) == false )
                    possibleConflicts = null; // we had a bloom check and it says we don't have a conflict
                else
                    possibleConflicts = table.getLatest(kvPair.getRowKey(), possibleConflicts);
                if (possibleConflicts != null && !possibleConflicts.isEmpty()) {
                    //we need to check for write conflicts
                    try {
                        conflictResults = ensureNoWriteConflict(transaction, writeType, possibleConflicts, rollForwardQueue, supplier);
                    } catch (IOException ioe) {
                        if (ioe instanceof WriteConflict) {
                            finalStatus[i] = operationStatusLib.failure(ioe);
                            continue;
                        } else throw ioe;
                    }
                    if (applyConstraint(constraintChecker, constraintStateFilter, i, kvPair, possibleConflicts,
                            finalStatus, conflictResults.hasAdditiveConflicts())) //filter this row out, it fails the constraint
                        continue;
                }
                //TODO -sf- if type is an UPSERT, and conflict type is ADDITIVE_CONFLICT, then we
                //set the status on the row to ADDITIVE_CONFLICT_DURING_UPSERT
                if(KVPair.Type.UPSERT.equals(writeType)){
                    /*
                     * If the type is an upsert, then we want to check for an ADDITIVE conflict. If so,
                     * we fail this row with an ADDITIVE_UPSERT_CONFLICT.
                     */
                    if (conflictResults.hasAdditiveConflicts()) {
                        finalStatus[i] = operationStatusLib.failure(exceptionLib.additiveWriteConflict());
                    }
                }
            }

            boolean addFirstOccurrenceToken = false;

            // todo DB-10761: improve this solution for the cases where we know that there's no conflicts and we don't need to check
            if (checkedConflicts) {
                if (possibleConflicts == null || possibleConflicts.isEmpty())
                {
                    // First write
                    if (KVPair.Type.INSERT.equals(writeType) || KVPair.Type.UPSERT.equals(writeType))
                        addFirstOccurrenceToken = true;
                } else if (KVPair.Type.DELETE.equals(writeType) && possibleConflicts.firstWriteToken() != null) {
                    // Delete following first write
                    assert possibleConflicts.userData() != null;
                    addFirstOccurrenceToken = possibleConflicts.firstWriteToken().version() == possibleConflicts.userData().version();
                }
            }

            MutationToRun mutationToRun = new MutationToRun();
            mutationToRun.data = getMutationToRun( table, kvPair, family, qualifier, transaction, conflictResults,
                    addFirstOccurrenceToken, skipWAL, toRollforward);
            mutationToRun.index = i;
            mutationToRun.conflictingChildren = conflictResults.getChildConflicts();

            mutationsToRun.add( mutationToRun );
        }
        if (toRollforward != null && toRollforward.size() > 0) {
            SIDriver.driver().getRollForward().submitForResolution(table,transaction.getTxnId(),toRollforward);
        }

        return mutationsToRun; //finalMutationsToWrite;
    }

    private boolean applyConstraint(ConstraintChecker constraintChecker,
                                    TxnFilter constraintStateFilter,
                                    int rowPosition,
                                    KVPair mutation,
                                    DataResult row,
                                    MutationStatus[] finalStatus,
                                    boolean additiveConflict) throws IOException{
        /*
         * Attempts to apply the constraint (if there is any). When this method returns true, the row should be filtered
         * out.
         */
        if(constraintChecker==null) return false;

        if(row==null || row.isEmpty()) return false;
        //must reset the filter here to avoid contaminating multiple rows with tombstones and stuff
        constraintStateFilter.reset();

        //we need to make sure that this row is visible to the current transaction
        List<DataCell> visibleColumns=Lists.newArrayListWithExpectedSize(row.size());
        for(DataCell data : row){
            DataFilter.ReturnCode code=constraintStateFilter.filterCell(data);
            switch(code){
                case NEXT_ROW:
                case NEXT_COL:
                case SEEK:
                    return false;
                case SKIP:
                    continue;
                default:
                    visibleColumns.add(data.getClone()); //TODO -sf- remove this clone
            }
        }
        constraintStateFilter.nextRow();
        if(!additiveConflict && visibleColumns.size()<=0) return false; //no visible values to check

        MutationStatus operationStatus=constraintChecker.checkConstraint(mutation,opFactory.newResult(visibleColumns));
        if(operationStatus!=null && !operationStatus.isSuccess()){
            finalStatus[rowPosition]=operationStatus;
            return true;
        }
        return false;
    }


    private void lockRows(Partition table,Collection<KVPair> mutations,Pair<KVPair, Lock>[] mutationsAndLocks,MutationStatus[] finalStatus) throws IOException{
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
        try{
            for(KVPair mutation : mutations){
                ByteSlice byteSlice=mutation.rowKeySlice();
                Lock lock=table.getRowLock(byteSlice.array(),byteSlice.offset(),byteSlice.length());//tableWriter.getRowLock(table, mutation.rowKeySlice());
                if(lock.tryLock())
                    mutationsAndLocks[position]=Pair.newPair(mutation,lock);
                else
                    finalStatus[position]=operationStatusLib.notRun();

                position++;
            }
        }catch(RuntimeException re){
            /*
             * trying the lock can result in us throwing a NotServingRegionException etc, which is wrapped
             * by the RuntimeException. Thus, we want to convert all RuntimeErrors to IOExceptions (if possible)
             *
             * todo -sf- I'm not sure if this is correct around OutOfMemoryError and other stuff like that
             */
            throw exceptionLib.processRemoteException(re);
        }
    }


    private DataPut getMutationToRun(Partition table, KVPair kvPair,
                                     byte[] family, byte[] column,
                                     TxnView transaction, ConflictResults conflictResults, boolean addFirstOccurrenceToken,
                                     boolean skipWAL, List<ByteSlice> rollforward) throws IOException{
        long txnIdLong=transaction.getTxnId();
//                if (LOG.isTraceEnabled()) LOG.trace(String.format("table = %s, kvPair = %s, txnId = %s", table.toString(), kvPair.toString(), txnIdLong));
        DataPut newPut;
        if(kvPair.getType()==KVPair.Type.EMPTY_COLUMN) {
            /*
             * WARNING: This requires a read of column data to populate! Try not to use
             * it unless no other option presents itself.
             *
             * In point of fact, this only occurs if someone sends over a non-delete Put
             * which has only SI data. In the event that we send over a row with all nulls
             * from actual Splice system, we end up with a KVPair that has a non-empty byte[]
             * for the values column (but which is nulls everywhere)
             */
            newPut=opFactory.newPut(kvPair.rowKeySlice());
            setTombstonesOnColumns(table,txnIdLong,newPut);
        } else if(kvPair.getType()==KVPair.Type.DELETE) {
            newPut=opFactory.newPut(kvPair.rowKeySlice());
            newPut.tombstone(txnIdLong);
            if (addFirstOccurrenceToken) {
                newPut.addDeleteRightAfterFirstWriteToken(family, txnIdLong);
            }
        } else {
            newPut = opFactory.toDataPut(kvPair, family, column, txnIdLong);
            if (addFirstOccurrenceToken) {
                newPut.addFirstWriteToken(family, txnIdLong);
            }
        }

        newPut.addAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
        if(kvPair.getType()!=KVPair.Type.DELETE && conflictResults.hasTombstone())
            newPut.antiTombstone(txnIdLong);

        if(rollforward != null)
            rollforward.add(kvPair.rowKeySlice());

        if (skipWAL)
            newPut.setDurability(Durability.NONE);
        return newPut;
    }

    private void resolveChildConflicts(Partition table,DataPut put,LongHashSet conflictingChildren) throws IOException{
        if(conflictingChildren!=null && !conflictingChildren.isEmpty()){
            DataDelete delete=opFactory.newDelete(put.key());
            Iterable<DataCell> cells=put.cells();
            for(LongCursor lc : conflictingChildren){
                for(DataCell dc : cells){
                    delete.deleteColumn(dc.family(),dc.qualifier(),lc.value);
                }
                delete.deleteColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.TOMBSTONE_COLUMN_BYTES,lc.value);
                delete.deleteColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.COMMIT_TIMESTAMP_COLUMN_BYTES,lc.value);
            }
            delete.addAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
            table.delete(delete);
        }
    }

    /**
     * While we hold the lock on the row, check to make sure that no transactions have updated the row since the
     * updating transaction started.
     */
    private ConflictResults ensureNoWriteConflict(TxnView updateTransaction, KVPair.Type updateType, DataResult row, ConflictRollForward conflictRollForward, TxnSupplier txnSupplier) throws IOException{

        DataCell commitTsKeyValue=row.commitTimestamp();

        long txnId = 0;
        long commitTs = 0;
        ConflictResults conflictResults=null;
        if(commitTsKeyValue!=null){
            conflictResults=checkCommitTimestampForConflict(updateTransaction,null,commitTsKeyValue,txnSupplier);
            txnId = commitTsKeyValue.version();
            if(commitTsKeyValue.valueLength()>0) {
                commitTs = commitTsKeyValue.valueAsLong();
            }
        }

        conflictRollForward.reset(txnId, commitTs);

        DataCell tombstoneKeyValue=row.tombstoneOrAntiTombstone();
        conflictRollForward.handle(tombstoneKeyValue);
        if(tombstoneKeyValue!=null){
            long dataTransactionId=tombstoneKeyValue.version();
            conflictResults=checkDataForConflict(updateTransaction,conflictResults,tombstoneKeyValue,dataTransactionId,txnSupplier);
            conflictResults=(conflictResults==null)?new ConflictResults():conflictResults;
            conflictResults.setHasTombstone(hasCurrentTransactionTombstone(updateTransaction,tombstoneKeyValue,txnSupplier));
        }
        DataCell userDataKeyValue=row.userData();
        conflictRollForward.handle(userDataKeyValue);
        if(userDataKeyValue!=null){
            long dataTransactionId=userDataKeyValue.version();
            conflictResults=checkDataForConflict(updateTransaction,conflictResults,userDataKeyValue,dataTransactionId,txnSupplier);
        }
        // FK counter -- can only conflict with DELETE
        if(updateType==KVPair.Type.DELETE){
            DataCell fkCounterKeyValue=row.fkCounter();
            conflictRollForward.handle(fkCounterKeyValue);
            if(fkCounterKeyValue!=null){
                long dataTransactionId=fkCounterKeyValue.valueAsLong();
                conflictResults=checkDataForConflict(updateTransaction,conflictResults,fkCounterKeyValue,dataTransactionId,txnSupplier);
            }
        }

        return conflictResults==null?ConflictResults.NO_CONFLICT:conflictResults;
    }

    private boolean hasCurrentTransactionTombstone(TxnView updateTxn,DataCell tombstoneCell,TxnSupplier txnSupplier) throws IOException{
        if(tombstoneCell==null) return false; //no tombstone at all
        if(tombstoneCell.dataType()==CellType.ANTI_TOMBSTONE) return false;
        TxnView tombstoneTxn=txnSupplier.getTransaction(tombstoneCell.version());
        return updateTxn.canSee(tombstoneTxn);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private ConflictResults checkCommitTimestampForConflict(TxnView updateTransaction,
                                                            ConflictResults conflictResults,
                                                            DataCell commitCell,
                                                            TxnSupplier txnSupplier) throws IOException{
//        final long dataTransactionId = opFactory.getTimestamp(dataCommitKeyValue);
        long txnId=commitCell.version();
        if(commitCell.valueLength()>0){
            long globalCommitTs=commitCell.valueAsLong();
            if(globalCommitTs<0){
                // Unknown transaction status
                final TxnView dataTransaction=txnSupplier.getTransaction(txnId);
                if(dataTransaction.getState()==Txn.State.ROLLEDBACK)
                    return conflictResults; //can't conflict with a rolled back transaction
                final ConflictType conflictType=updateTransaction.conflicts(dataTransaction);
                switch(conflictType){
                    case CHILD:
                        if(conflictResults==null)
                            conflictResults=new ConflictResults();
                        conflictResults.addChild(txnId);
                        break;
                    case ADDITIVE:
                        if(conflictResults==null)
                            conflictResults=new ConflictResults();
                        conflictResults.addAdditive(txnId);
                        break;
                    case SIBLING:
                        if(LOG.isTraceEnabled()){
                            SpliceLogUtils.trace(LOG,"Write conflict on row "
                                    +Bytes.toHex(commitCell.keyArray(),commitCell.keyOffset(),commitCell.keyLength()));
                        }

                        throw exceptionLib.writeWriteConflict(txnId,updateTransaction.getTxnId());
                }
            }else{
                // Committed transaction
                if(globalCommitTs>updateTransaction.getBeginTimestamp()){
                    if(LOG.isTraceEnabled()){
                        SpliceLogUtils.trace(LOG,"Write conflict on row "
                                +Bytes.toHex(commitCell.keyArray(),commitCell.keyOffset(),commitCell.keyLength()));
                    }
                    throw exceptionLib.writeWriteConflict(txnId,updateTransaction.getTxnId());
                }
            }
        }
        return conflictResults;
    }

    private ConflictResults checkDataForConflict(TxnView updateTransaction,
                                                 ConflictResults conflictResults,
                                                 DataCell cell,
                                                 long dataTransactionId,
                                                 TxnSupplier txnSupplier) throws IOException{
        if(updateTransaction.getTxnId()!=dataTransactionId){
            final TxnView dataTransaction=txnSupplier.getTransaction(dataTransactionId);
            if(dataTransaction.getState()==Txn.State.ROLLEDBACK){
                if (dataTransaction.getEffectiveBeginTimestamp() > updateTransaction.getEffectiveBeginTimestamp()) {
                    // If we ignore this transaction it could mask other writes with which we do conflict, see DB-7582 and DB-7779 for details
                    if(LOG.isTraceEnabled()){
                        SpliceLogUtils.trace(LOG,"Write conflict on row "
                                +Bytes.toHex(cell.keyArray(),cell.keyOffset(),cell.keyLength()));
                    }
                    throw exceptionLib.writeWriteConflict(dataTransactionId,updateTransaction.getTxnId());
                }
                return conflictResults; //can't conflict with a rolled back transaction
            }
            final ConflictType conflictType=updateTransaction.conflicts(dataTransaction);
            switch(conflictType){
                case CHILD:
                    if(conflictResults==null){
                        conflictResults=new ConflictResults();
                    }
                    conflictResults.addChild(dataTransactionId);
                    break;
                case ADDITIVE:
                    if(conflictResults==null){
                        conflictResults=new ConflictResults();
                    }
                    conflictResults.addAdditive(dataTransactionId);
                    break;
                case SIBLING:
                    if(LOG.isTraceEnabled()){
                        SpliceLogUtils.trace(LOG,"Write conflict on row "
                                +Bytes.toHex(cell.keyArray(),cell.keyOffset(),cell.keyLength()));
                    }
                    throw exceptionLib.writeWriteConflict(dataTransactionId,updateTransaction.getTxnId());
            }
        }
        return conflictResults;
    }

    // Helpers

    private boolean isFlaggedForSITreatment(Attributable operation){
        return operation.getAttribute(SIConstants.SI_NEEDED)!=null;
    }


    private void ensureTransactionAllowsWrites(TxnView transaction) throws IOException{
        if(transaction==null || !transaction.allowsWrites()){
            throw exceptionLib.readOnlyModification("transaction is read only: "+transaction);
        }
    }

    private void setTombstonesOnColumns(Partition table, long timestamp, DataPut put) throws IOException {
        //-sf- this doesn't really happen in practice, it's just for a safety valve, which is good, cause it's expensive
        final Map<byte[], byte[]> userData = getUserData(table,put.key());
        if (userData != null) {
            for (byte[] qualifier : userData.keySet()) {
                put.addCell(SIConstants.PACKED_COLUMN_BYTES,qualifier,timestamp,SIConstants.EMPTY_BYTE_ARRAY);
            }
        }
    }

    private Map<byte[], byte[]> getUserData(Partition table, byte[] rowKey) throws IOException {
        DataResult dr = table.getLatest(rowKey,SIConstants.PACKED_COLUMN_BYTES,null);
        if (dr != null) {
            return dr.familyCellMap(SIConstants.PACKED_COLUMN_BYTES);
        }
        return null;
    }
}
