package com.splicemachine.hbase.writer;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class TransactionalUnsafeCallBuffer<E> extends UnsafeCallBuffer<E> implements TransactionalCallBuffer<E>{
    private final String transactionId;

    public TransactionalUnsafeCallBuffer(String transactionId,long maxHeapSize, int maxBufferEntries, Listener<E> listener) {
        super(maxHeapSize, maxBufferEntries, listener);
        this.transactionId = transactionId;
    }

    @Override
    public String getTransactionId() {
        return transactionId;
    }
}
