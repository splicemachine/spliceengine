package com.splicemachine.si.impl.functions;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.Record;
import com.splicemachine.storage.RecordType;
import org.spark_project.guava.base.Function;
import javax.annotation.Nullable;
import java.util.BitSet;
import java.util.concurrent.locks.Lock;

/**
 *
 * Perform Possible Conflict Check
 *
 * Should not be applied when insert without constraints (no pk)
 *
 */
public class PossibleConflictCheck implements Function<Record[],IntObjectOpenHashMap<Record>> {
    private Partition table;
    private Lock[] locks;
    private boolean hasConstraintChecker;


    public PossibleConflictCheck(Partition table, Lock[] locks, boolean hasConstraintChecker) {
        this.table = table;
        this.locks = locks;
        this.hasConstraintChecker = hasConstraintChecker;
    }

    @Nullable
    @Override
    public IntObjectOpenHashMap<Record> apply(Record[] records) {
        try {
            BitSet bloomInMemoryCheck  = table.getBloomInMemoryCheck(records, locks);
            IntObjectOpenHashMap<Record> possibleConflicts = null;
            for (int i = 0; i< records.length; i++) {
                if(locks[i]==null) continue; // Cannot Check, since lock failed
                Record record = records[i];
                if (RecordType.INSERT.equals(record.getRecordType()) && !hasConstraintChecker)
                    continue;
                Record conflictRecord=bloomInMemoryCheck==null||bloomInMemoryCheck.get(i)?table.getLatest(record.getKey()):null;
                if (conflictRecord != null) {
                    if (possibleConflicts == null)
                        possibleConflicts = IntObjectOpenHashMap.newInstance();
                    possibleConflicts.put(i,conflictRecord);
                }
            }
            return possibleConflicts;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
