package com.splicemachine.storage.util;

import org.sparkproject.guava.base.Predicate;
import com.splicemachine.storage.Partition;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nullable;

/**
 * Simple Predicate to test whether a partition is in range.
 */
public class PartitionInRangePredicate implements Predicate<Partition> {
    private byte[] startKey;
    private byte[] endKey;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public PartitionInRangePredicate(byte[] startKey, byte[] endKey) {
            this.startKey = startKey;
            this.endKey = endKey;
    }
    @Override
    public boolean apply(@Nullable Partition partition) {
        assert partition!=null: "Partition cannot be null";
        return partition.overlapsRange(startKey,endKey);
    }
}
