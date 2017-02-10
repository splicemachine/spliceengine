/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.*;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.readresolve.RollForward;
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
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.storage.*;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.spark_project.guava.base.Predicate;
import org.spark_project.guava.collect.Collections2;
import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.collect.Maps;
import javax.annotation.Nullable;
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

    private final TxnOperationFactory txnOperationFactory;
    private final TxnSupplier txnSupplier;

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
    }

    // Operation pre-processing. These are to be called "server-side" when we are about to process an operation.

    // Process update operations

    @Override
    public boolean processPut(Partition table,RollForward rollForwardQueue,DataPut put) throws IOException{
        if(!isFlaggedForSITreatment(put)) return false;
        final DataPut[] mutations=new DataPut[]{put};
        mutations[0]=put;
        MutationStatus[] operationStatuses=processPutBatch(table,rollForwardQueue,mutations);
        return operationStatusLib.processPutStatus(operationStatuses[0]);
    }

    @Override
    public MutationStatus[] processPutBatch(Partition table,RollForward rollForwardQueue,DataPut[] mutations) throws IOException{
        if(mutations.length==0){
            //short-circuit special case of empty batch
            //noinspection unchecked
            return new MutationStatus[0];
        }

        Map<Long, Map<byte[], Map<byte[], List<KVPair>>>> kvPairMap=SITransactorUtil.putToKvPairMap(mutations,txnOperationFactory);
        final Map<byte[], MutationStatus> statusMap= Maps.newTreeMap(Bytes.BASE_COMPARATOR);
        for(Map.Entry<Long, Map<byte[], Map<byte[], List<KVPair>>>> entry : kvPairMap.entrySet()){
            long txnId=entry.getKey();
            Map<byte[], Map<byte[], List<KVPair>>> familyMap=entry.getValue();
            for(Map.Entry<byte[], Map<byte[], List<KVPair>>> familyEntry : familyMap.entrySet()){
                byte[] family=familyEntry.getKey();
                Map<byte[], List<KVPair>> columnMap=familyEntry.getValue();
                for(Map.Entry<byte[], List<KVPair>> columnEntry : columnMap.entrySet()){
                    byte[] qualifier=columnEntry.getKey();
                    List<KVPair> kvPairs= Lists.newArrayList(Collections2.filter(columnEntry.getValue(), new Predicate<KVPair>() {
                        @Override
                        public boolean apply(@Nullable KVPair input) {
                            assert input != null;
                            return !statusMap.containsKey(input.getRowKey()) || statusMap.get(input.getRowKey()).isSuccess();
                        }
                    }));
                    MutationStatus[] statuses=processKvBatch(table,null,family,qualifier,kvPairs,txnId,operationStatusLib.getNoOpConstraintChecker());
                    for(int i=0;i<statuses.length;i++){
                        byte[] row=kvPairs.get(i).getRowKey();
                        MutationStatus status=statuses[i];
                        if(statusMap.containsKey(row)){
                            MutationStatus oldStatus=statusMap.get(row);
                            status=getCorrectStatus(status,oldStatus);
                        }
                        statusMap.put(row,status);
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
                                            long txnId,
                                            ConstraintChecker constraintChecker) throws IOException{
        TxnView txn=txnSupplier.getTransaction(txnId);
        return processKvBatch(table,rollForward,defaultFamilyBytes,packedColumnBytes,toProcess,txn,constraintChecker);
    }

    @Override
    public MutationStatus[] processKvBatch(Partition table,
                                           RollForward rollForward,
                                           byte[] defaultFamilyBytes,
                                           byte[] packedColumnBytes,
                                           Collection<KVPair> toProcess,
                                           TxnView txn,
                                           ConstraintChecker constraintChecker) throws IOException{
        ensureTransactionAllowsWrites(txn);
        return processInternal(table,rollForward,txn,defaultFamilyBytes,packedColumnBytes,toProcess,constraintChecker);
    }

    private MutationStatus getCorrectStatus(MutationStatus status,MutationStatus oldStatus){
        return operationStatusLib.getCorrectStatus(status,oldStatus);
    }

    private MutationStatus[] processInternal(Partition table,
                                             RollForward rollForwardQueue,
                                             TxnView txn,
                                             byte[] family,byte[] qualifier,
                                             Collection<KVPair> mutations,
                                             ConstraintChecker constraintChecker) throws IOException{
//                if (LOG.isTraceEnabled()) LOG.trace(String.format("processInternal: table = %s, txnId = %s", table.toString(), txn.getTxnId()));
        MutationStatus[] finalStatus=new MutationStatus[mutations.size()];
        Pair<KVPair, Lock>[] lockPairs=new Pair[mutations.size()];
        TxnFilter constraintState=null;
        if(constraintChecker!=null)
            constraintState=new SimpleTxnFilter(null,txn,NoOpReadResolver.INSTANCE,txnSupplier);
        @SuppressWarnings("unchecked") final LongOpenHashSet[] conflictingChildren=new LongOpenHashSet[mutations.size()];
        try{
            lockRows(table,mutations,lockPairs,finalStatus);

            /*
             * You don't need a low-level operation check here, because this code can only be called from
             * 1 of 2 paths (bulk write pipeline and SIObserver). Since both of those will externally ensure that
             * the region can't close until after this method is complete, we don't need the calls.
             */
            IntObjectOpenHashMap<DataPut> writes=checkConflictsForKvBatch(table,rollForwardQueue,lockPairs,
                    conflictingChildren,txn,family,qualifier,constraintChecker,constraintState,finalStatus);

            //TODO -sf- this can probably be made more efficient
            //convert into array for usefulness
            DataPut[] toWrite=new DataPut[writes.size()];
            int i=0;
            for(IntObjectCursor<DataPut> write : writes){
                toWrite[i]=write.value;
                i++;
            }
            Iterator<MutationStatus> status=table.writeBatch(toWrite);

            //convert the status back into the larger array
            i=0;
            for(IntObjectCursor<DataPut> write : writes){
                if(!status.hasNext())
                    throw new IllegalStateException("Programmer Error: incorrect length for returned status");
                finalStatus[write.key]=status.next().getClone(); //TODO -sf- is clone needed here?
                //resolve child conflicts
                try{
                    resolveChildConflicts(table,write.value,conflictingChildren[i]);
                }catch(Exception e){
                    finalStatus[i] = operationStatusLib.failure(e);
                }
                i++;
            }
            return finalStatus;
        }finally{
            releaseLocksForKvBatch(lockPairs);
        }
    }

    private void releaseLocksForKvBatch(Pair<KVPair, Lock>[] locks){
        if(locks==null) return;
        for(Pair<KVPair, Lock> lock : locks){
            if(lock==null || lock.getSecond()==null) continue;
            lock.getSecond().unlock();
        }
    }

    private IntObjectOpenHashMap<DataPut> checkConflictsForKvBatch(Partition table,
                                                                   RollForward rollForwardQueue,
                                                                   Pair<KVPair, Lock>[] dataAndLocks,
                                                                   LongOpenHashSet[] conflictingChildren,
                                                                   TxnView transaction,
                                                                   byte[] family,byte[] qualifier,
                                                                   ConstraintChecker constraintChecker,
                                                                   TxnFilter constraintStateFilter,
                                                                   MutationStatus[] finalStatus) throws IOException {
        IntObjectOpenHashMap<DataPut> finalMutationsToWrite = IntObjectOpenHashMap.newInstance(dataAndLocks.length, 0.9f);
        DataResult possibleConflicts = null;
        BitSet bloomInMemoryCheck  = table.getBloomInMemoryCheck(constraintChecker!=null,dataAndLocks);
        for(int i=0;i<dataAndLocks.length;i++){
            Pair<KVPair, Lock> baseDataAndLock=dataAndLocks[i];
            if(baseDataAndLock==null) continue;

            ConflictResults conflictResults=ConflictResults.NO_CONFLICT;
            KVPair kvPair=baseDataAndLock.getFirst();
            KVPair.Type writeType=kvPair.getType();
            if(constraintChecker!=null || !KVPair.Type.INSERT.equals(writeType)){
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
                possibleConflicts=bloomInMemoryCheck==null||bloomInMemoryCheck.get(i)?table.getLatest(kvPair.getRowKey(),possibleConflicts):null;
                if(possibleConflicts!=null){
                    //we need to check for write conflicts
                    try {
                        conflictResults = ensureNoWriteConflict(transaction, writeType, possibleConflicts);
                    } catch (IOException ioe) {
                        if (ioe instanceof WriteConflict) {
                            finalStatus[i] = operationStatusLib.failure(ioe);
                            continue;
                        } else throw ioe;
                    }
                    if(applyConstraint(constraintChecker,constraintStateFilter,i,kvPair,possibleConflicts,finalStatus,conflictResults.hasAdditiveConflicts())) //filter this row out, it fails the constraint
                        continue;
                }
                //TODO -sf- if type is an UPSERT, and conflict type is ADDITIVE_CONFLICT, then we
                //set the status on the row to ADDITIVE_CONFLICT_DURING_UPSERT
                if(KVPair.Type.UPSERT.equals(writeType)){
                    /*
                     * If the type is an upsert, then we want to check for an ADDITIVE conflict. If so,
                     * we fail this row with an ADDITIVE_UPSERT_CONFLICT.
                     */
                    if(conflictResults.hasAdditiveConflicts()){
                        finalStatus[i]=operationStatusLib.failure(exceptionLib.additiveWriteConflict());
                    }
                }
            }

            conflictingChildren[i]=conflictResults.getChildConflicts();
            DataPut mutationToRun=getMutationToRun(table,rollForwardQueue,kvPair,
                    family,qualifier,transaction,conflictResults);
            finalMutationsToWrite.put(i,mutationToRun);
        }
        return finalMutationsToWrite;
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

        if(row==null || row.size()<=0) return false;
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


    private DataPut getMutationToRun(Partition table,RollForward rollForwardQueue,KVPair kvPair,
                                     byte[] family,byte[] column,
                                     TxnView transaction,ConflictResults conflictResults) throws IOException{
        long txnIdLong=transaction.getTxnId();
//                if (LOG.isTraceEnabled()) LOG.trace(String.format("table = %s, kvPair = %s, txnId = %s", table.toString(), kvPair.toString(), txnIdLong));
        DataPut newPut;
        if(kvPair.getType()==KVPair.Type.EMPTY_COLUMN){
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
        }else if(kvPair.getType()==KVPair.Type.DELETE){
            newPut=opFactory.newPut(kvPair.rowKeySlice());
            newPut.tombstone(txnIdLong);
        }else
            newPut=opFactory.toDataPut(kvPair,family,column,txnIdLong);

        newPut.addAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
        if(kvPair.getType()!=KVPair.Type.DELETE && conflictResults.hasTombstone())
            newPut.antiTombstone(txnIdLong);

        if(rollForwardQueue!=null)
            rollForwardQueue.submitForResolution(kvPair.rowKeySlice(),txnIdLong);
        return newPut;
    }

    private void resolveChildConflicts(Partition table,DataPut put,LongOpenHashSet conflictingChildren) throws IOException{
        if(conflictingChildren!=null && !conflictingChildren.isEmpty()){
            DataDelete delete=opFactory.newDelete(put.key());
            Iterable<DataCell> cells=put.cells();
            for(LongCursor lc : conflictingChildren){
                for(DataCell dc : cells){
                    delete.deleteColumn(dc.family(),dc.qualifier(),lc.value);
                }
                delete.deleteColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,lc.value);
                delete.deleteColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,lc.value);
            }
            delete.addAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
            table.delete(delete);
        }
    }

    /**
     * While we hold the lock on the row, check to make sure that no transactions have updated the row since the
     * updating transaction started.
     */
    private ConflictResults ensureNoWriteConflict(TxnView updateTransaction,KVPair.Type updateType,DataResult row) throws IOException{

        DataCell commitTsKeyValue=row.commitTimestamp();//opFactory.getColumnLatest(result, DEFAULT_FAMILY_BYTES, SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES);
//        Data tombstoneKeyValue = opFactory.getColumnLatest(result, DEFAULT_FAMILY_BYTES, SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES);
//        Data userDataKeyValue = opFactory.getColumnLatest(result, DEFAULT_FAMILY_BYTES, PACKED_COLUMN_BYTES);

        ConflictResults conflictResults=null;
        if(commitTsKeyValue!=null){
            conflictResults=checkCommitTimestampForConflict(updateTransaction,null,commitTsKeyValue);
        }
        DataCell tombstoneKeyValue=row.tombstone();
        if(tombstoneKeyValue!=null){
            long dataTransactionId=tombstoneKeyValue.version();
            conflictResults=checkDataForConflict(updateTransaction,conflictResults,tombstoneKeyValue,dataTransactionId);
            conflictResults=(conflictResults==null)?new ConflictResults():conflictResults;
            conflictResults.setHasTombstone(hasCurrentTransactionTombstone(updateTransaction,tombstoneKeyValue));
        }
        DataCell userDataKeyValue=row.userData();
        if(userDataKeyValue!=null){
            long dataTransactionId=userDataKeyValue.version();
            conflictResults=checkDataForConflict(updateTransaction,conflictResults,userDataKeyValue,dataTransactionId);
        }
        // FK counter -- can only conflict with DELETE
        if(updateType==KVPair.Type.DELETE){
            DataCell fkCounterKeyValue=row.fkCounter();
            if(fkCounterKeyValue!=null){
                long dataTransactionId=fkCounterKeyValue.valueAsLong();
                conflictResults=checkDataForConflict(updateTransaction,conflictResults,fkCounterKeyValue,dataTransactionId);
            }
        }

        return conflictResults==null?ConflictResults.NO_CONFLICT:conflictResults;
    }

    private boolean hasCurrentTransactionTombstone(TxnView updateTxn,DataCell tombstoneCell) throws IOException{
        if(tombstoneCell==null) return false; //no tombstone at all
        if(tombstoneCell.dataType()==CellType.ANTI_TOMBSTONE) return false;
        TxnView tombstoneTxn=txnSupplier.getTransaction(tombstoneCell.version());
        return updateTxn.canSee(tombstoneTxn);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private ConflictResults checkCommitTimestampForConflict(TxnView updateTransaction,
                                                            ConflictResults conflictResults,
                                                            DataCell commitCell) throws IOException{
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
                                                 long dataTransactionId) throws IOException{
        if(updateTransaction.getTxnId()!=dataTransactionId){
            final TxnView dataTransaction=txnSupplier.getTransaction(dataTransactionId);
            if(dataTransaction.getState()==Txn.State.ROLLEDBACK){
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
