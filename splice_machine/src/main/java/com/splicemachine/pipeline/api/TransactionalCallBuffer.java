package com.splicemachine.pipeline.api;


/**
 * 
 * CallBuffer that allows you to retrieve a transaction id.
 * 
 * @author Scott Fines
 * Created on: 8/8/13
 */
public interface TransactionalCallBuffer<E> extends CallBuffer<E> {

    public String getTransactionId();
}
