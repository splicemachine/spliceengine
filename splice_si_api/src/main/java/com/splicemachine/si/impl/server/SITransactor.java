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
import com.google.common.collect.Lists;
import com.splicemachine.si.api.data.*;
import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.api.txn.*;
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

            Iterator<MutationStatus> status = table.writeBatch(Lists.newArrayList(toProcess));

            return finalStatus;
        }finally{
            if (locks!=null)
                new ReleaseLocks().apply(locks);
        }
    }


}
