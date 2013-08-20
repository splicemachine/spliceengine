package com.splicemachine.si.impl;

/**
 * Simple data holder for items related to a Put while it is being processed as part of a batch.
 */
public class PutInBatch<Data, Put> {
    final ImmutableTransaction transaction;
    final Data rowKey;
    final int index;
    final Put put;

    public PutInBatch(ImmutableTransaction transaction, Data rowKey, int index, Put put) {
        this.transaction = transaction;
        this.rowKey = rowKey;
        this.index = index;
        this.put = put;
    }
}
