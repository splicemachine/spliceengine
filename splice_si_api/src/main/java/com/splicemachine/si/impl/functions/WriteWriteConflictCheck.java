package com.splicemachine.si.impl.functions;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.txn.ConflictType;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.Record;
import org.spark_project.guava.base.Function;
import javax.annotation.Nullable;

/**
 *
 * Perform Possible Conflict Check
 *
 * Should not be applied when insert without constraints (no pk)
 *
 */
public class WriteWriteConflictCheck implements Function<IntObjectOpenHashMap<Record>,
        IntObjectOpenHashMap<ConflictType>> {
    private Partition table;
    private Record[] records;
    private TxnOperationFactory txnOperationFactory;


    public WriteWriteConflictCheck(Partition table, Record[] records, TxnOperationFactory txnOperationFactory) {
        this.table = table;
        this.records = records;
        this.txnOperationFactory = txnOperationFactory;
    }

    @Nullable
    @Override
    public IntObjectOpenHashMap<ConflictType> apply(IntObjectOpenHashMap<Record> possibleConflicts) {
            //we need to check for write conflicts
            IntObjectOpenHashMap<ConflictType> conflictResults = null;
            for (int i : possibleConflicts.keys) {
                ConflictType conflictType = txnOperationFactory.conflicts(records[i], possibleConflicts.get(i));
                if (!conflictType.equals(ConflictType.NONE)) {
                    if (conflictResults == null)
                        conflictResults = IntObjectOpenHashMap.newInstance();
                    conflictResults.put(i,conflictType);
                }
            }
            return conflictResults;
        }
}