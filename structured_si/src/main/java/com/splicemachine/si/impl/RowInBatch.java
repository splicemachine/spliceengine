package com.splicemachine.si.impl;

/**
 * Simple data holder for a row that is being processed as part of a batch put operation.
 */
public class RowInBatch<Data, Hashable> {
    final ImmutableTransaction transaction;
    final Data rowKey;
    final Hashable hashableRowKey;

    public RowInBatch(ImmutableTransaction transaction, Data rowKey, Hashable hashableRowKey) {
        this.transaction = transaction;
        this.rowKey = rowKey;
        this.hashableRowKey = hashableRowKey;
    }
}
