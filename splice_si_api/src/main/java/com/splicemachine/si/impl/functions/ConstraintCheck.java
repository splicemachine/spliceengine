package com.splicemachine.si.impl.functions;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.storage.MutationStatus;
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
public class ConstraintCheck implements Function<IntObjectOpenHashMap<Record>,
        IntObjectOpenHashMap<MutationStatus>> {
    private Record[] records;
    private ConstraintChecker constraintChecker;


    public ConstraintCheck(Record[] records, ConstraintChecker constraintChecker) {
        this.records = records;
        this.constraintChecker = constraintChecker;
    }

    @Nullable
    @Override
    public IntObjectOpenHashMap<MutationStatus> apply(IntObjectOpenHashMap<Record> potentialConflicts) {
        try {
            IntObjectOpenHashMap<MutationStatus> constraintViolations = null;
            for (int i : potentialConflicts.keys) {
                MutationStatus status = constraintChecker.checkConstraint(potentialConflicts.get(i), records[i]);
                if (status != null && !status.isSuccess()) {
                    if (constraintViolations == null)
                        constraintViolations = IntObjectOpenHashMap.newInstance();
                    constraintViolations.put(i, status);
                }
            }
            return constraintViolations;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
