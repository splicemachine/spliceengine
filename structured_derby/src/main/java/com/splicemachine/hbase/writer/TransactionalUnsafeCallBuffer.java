package com.splicemachine.hbase.writer;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class TransactionalUnsafeCallBuffer<E> extends UnsafeCallBuffer<E> implements TransactionalCallBuffer<E>{
    private final String transactionId;

    public TransactionalUnsafeCallBuffer(String transactionId,BufferConfiguration bufferConfiguration, Listener<E> listener) {
        super(bufferConfiguration, listener);
        this.transactionId = transactionId;
    }

    @Override
    public String getTransactionId() {
        return transactionId;
    }
}
