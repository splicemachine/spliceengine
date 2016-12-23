package com.splicemachine.si.impl.functions;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.si.api.txn.WriteConflict;
import com.splicemachine.si.impl.ConflictResults;
import com.splicemachine.storage.MutationStatus;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.Record;
import com.splicemachine.storage.RecordType;
import org.spark_project.guava.base.Function;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.BitSet;
import java.util.concurrent.locks.Lock;

/**
 *
 * Perform Bloom Filter Check on records.
 *
 */
public class SnapshotIsolationConflictCheck implements Function<Record[],BitSet> {
    private Partition table;
    private  MutationStatus[] finalStatus;
    private ConstraintChecker constraintChecker;
    private Lock[] locks;
    private BitSet bloomInMemoryCheck;

    public SnapshotIsolationConflictCheck(Partition table, ConstraintChecker constraintChecker, Lock[] locks,
                                          BitSet bloomInMemoryCheck) {
        this.table = table;
        this.constraintChecker = constraintChecker;
        this.locks = locks;
        this.bloomInMemoryCheck = bloomInMemoryCheck;
    }

    @Nullable
    @Override
    public BitSet apply(Record[] records) {
        IntObjectOpenHashMap<DataPut> finalMutationsToWrite = IntObjectOpenHashMap.newInstance(locks.length, 0.9f);
        DataResult possibleConflicts = null;
        for (int i = 0; i< records.length; i++) {
            if (locks[i] == null) // Ignore Records We Could Not Get a lock on...
                continue;
            ConflictResults conflictResults=ConflictResults.NO_CONFLICT;
            Record record = records[i];
            RecordType recordType = record.getRecordType();
            // No Constraint Checker means no primary key, cannot conflict since the keys cannot overlap
            // via SnowFlake Mechanism
            if(constraintChecker!=null || !RecordType.INSERT.equals(recordType)){
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
                if(applyConstraint(constraintChecker,i,kvPair,possibleConflicts,finalStatus,conflictResults.hasAdditiveConflicts())) //filter this row out, it fails the constraint
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
        DataPut mutationToRun=getMutationToRun(table,kvPair,
                family,qualifier,transaction,conflictResults);
        finalMutationsToWrite.put(i,mutationToRun);
    }
    return finalMutationsToWrite;


        try {
            return table.getBloomInMemoryCheck(constraintChecker != null, records, locks);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
