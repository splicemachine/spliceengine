package com.splicemachine.si.api;

/**
 * @author Scott Fines
 * Created on: 8/21/13
 */
public interface RollForwardQueue {

    void start();

    void stop();

    void recordRow(long transactionId, byte[] rowKey, Boolean knownToBeCommitted);

    int getCount();
}
