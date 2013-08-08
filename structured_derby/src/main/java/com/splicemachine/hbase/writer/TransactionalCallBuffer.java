package com.splicemachine.hbase.writer;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public interface TransactionalCallBuffer<E> extends CallBuffer<E> {

    public String getTransactionId();
}
