package com.splicemachine.storage.util;

import com.google.common.base.Predicate;
import com.splicemachine.storage.Partition;

import javax.annotation.Nullable;

/**
 * Simple Predicate to test whether a partition is in range.
 */
public class PartitionInRangePredicate implements Predicate<Partition> {
    private byte[] startKey;
    private byte[] endKey;

    public PartitionInRangePredicate(byte[] startKey, byte[] endKey) {
            this.startKey = startKey;
            this.endKey = endKey;
    }
    @Override
    public boolean apply(@Nullable Partition partition) {
        return partition.overlapsRange(startKey,endKey);
    }
}
