/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.si.impl.server;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.splicemachine.si.api.data.*;
import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.api.txn.*;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.ConflictResults;
import com.splicemachine.si.impl.functions.*;
import com.splicemachine.storage.*;
import org.apache.log4j.Logger;
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
    private final OperationStatusFactory operationStatusLib;
    private final ExceptionFactory exceptionLib;
    private final TxnOperationFactory txnOperationFactory;
    private final TxnSupplier txnSupplier;

    public SITransactor(TxnSupplier txnSupplier,
                        TxnOperationFactory txnOperationFactory,
                        OperationStatusFactory operationStatusLib,
                        ExceptionFactory exceptionFactory){
        this.txnSupplier=txnSupplier;
        this.txnOperationFactory=txnOperationFactory;
        this.operationStatusLib = operationStatusLib;
        this.exceptionLib = exceptionFactory;
    }

    // Operation pre-processing. These are to be called "server-side" when we are about to process an operation.

    // Process update operations

    @Override
    public boolean processRecord(Partition table,Record record) throws IOException {
        final Record[] mutations=new Record[]{record};
        MutationStatus[] operationStatuses=processRecordBatch(table,mutations);
        return operationStatusLib.processPutStatus(operationStatuses[0]);
    }

    @Override
    public MutationStatus[] processRecordBatch(Partition table,Record[] records) throws IOException{
        if(records.length==0)
            return new MutationStatus[0];
        Txn txn = txnSupplier.getTransaction(records[0].getTxnId1());
        // JL-TODO
        return processRecordBatch(table,records,txn,IsolationLevel.SNAPSHOT_ISOLATION,operationStatusLib.getNoOpConstraintChecker());
    }

    /**
     *
     * This operation is under a control on the server side preventing region close...
     *
      * @param table
     * @param toProcess
     * @param txn
     * @param constraintChecker
     * @return
     * @throws IOException
     */
    @Override
    public MutationStatus[] processRecordBatch(Partition table,
                                           Record[] toProcess,
                                           Txn txn,
                                           IsolationLevel isolationLevel,
                                           ConstraintChecker constraintChecker) throws IOException{
        if(toProcess.length==0)
            return new MutationStatus[0];
        MutationStatus[] finalStatus=new MutationStatus[toProcess.length];
        @SuppressWarnings("unchecked") final LongOpenHashSet[] conflictingChildren=new LongOpenHashSet[toProcess.length];
        Lock[] locks = null;
        try{
            // Apply Lock Function
            locks = new ObtainLocks(table,finalStatus,operationStatusLib).apply(toProcess);
            IntObjectOpenHashMap<Record> possibleConflicts = new PossibleConflictCheck(table, locks,constraintChecker!=null).apply(toProcess);
            IntObjectOpenHashMap<ConflictType> conflicts = new WriteWriteConflictCheck(table,toProcess,txnOperationFactory).apply(possibleConflicts);
            IntObjectOpenHashMap<MutationStatus> constraints = constraintChecker==null?null:new ConstraintCheck(toProcess, constraintChecker).apply(possibleConflicts);

            List<Record>

            Iterator<MutationStatus> status = table.writeBatch(toProcess);

            return finalStatus;
        }finally{
            if (locks!=null)
                new ReleaseLocks().apply(locks);
        }
    }

    private IntObjectOpenHashMap<Record> checkConflictsForKvBatch(Partition table,
                                                                   Record[] records,
                                                                   Lock[] locks,
                                                                   LongOpenHashSet[] conflictingChildren,
                                                                   Txn txn,
                                                                   ConstraintChecker constraintChecker,
                                                                   MutationStatus[] finalStatus) throws IOException {
// ?        IntObjectOpenHashMap<Record> finalMutationsToWrite = IntObjectOpenHashMap.newInstance(locks.length, 0.9f);

        IntObjectOpenHashMap<Record> possibleConflicts = new PossibleConflictCheck(table, locks,constraintChecker!=null).apply(records);
        IntObjectOpenHashMap<ConflictType> conflicts = new WriteWriteConflictCheck(table,records,txnOperationFactory).apply(possibleConflicts);
        IntObjectOpenHashMap<MutationStatus> constraints = new ConstraintCheck(records, constraintChecker).apply(possibleConflicts);
                if (applyConstraint(constraintChecker, i, record, possibleConflicts, finalStatus, conflictResults.hasAdditiveConflicts())) //filter this row out, it fails the constraint
                    continue;

            //TODO -sf- if type is an UPSERT, and conflict type is ADDITIVE_CONFLICT, then we
            //set the status on the row to ADDITIVE_CONFLICT_DURING_UPSERT
            if (KVPair.Type.UPSERT.equals(writeType)) {
                    /*
                     * If the type is an upsert, then we want to check for an ADDITIVE conflict. If so,
                     * we fail this row with an ADDITIVE_UPSERT_CONFLICT.
                     */

/*                if (conflictResults.hasAdditiveConflicts()) {
                    finalStatus[i] = operationStatusLib.failure(exceptionLib.additiveWriteConflict());
                }
            }
        }

        }
    */
            conflictingChildren[i]=conflictResults.getChildConflicts();
            DataPut mutationToRun=getMutationToRun(table,kvPair,
                    family,qualifier,transaction,conflictResults);
            finalMutationsToWrite.put(i,mutationToRun);
        }
        return finalMutationsToWrite;
    }


    private DataPut getMutationToRun(Partition table,KVPair kvPair,
                                     byte[] family,byte[] column,
                                     Txn transaction,ConflictResults conflictResults) throws IOException{

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


}
