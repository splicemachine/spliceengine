package com.splicemachine.si.impl.functions;

import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.storage.MutationStatus;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.Record;
import org.spark_project.guava.base.Function;
import javax.annotation.Nullable;
import java.util.BitSet;
import java.util.concurrent.locks.Lock;

/**
 *
 * Perform Bloom Filter Check on records.
 *
 */
public class BloomFilterCheck implements Function<Record[],BitSet> {
    private Partition table;
    private  MutationStatus[] finalStatus;
    private ConstraintChecker constraintChecker;
    private Lock[] locks;


    public BloomFilterCheck(Partition table, ConstraintChecker constraintChecker, Lock[] locks) {
        this.table = table;
        this.constraintChecker = constraintChecker;
        this.locks = locks;
    }

    @Nullable
    @Override
    public BitSet apply(Record[] records) {
        try {
            return table.getBloomInMemoryCheck(constraintChecker != null, records, locks);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
