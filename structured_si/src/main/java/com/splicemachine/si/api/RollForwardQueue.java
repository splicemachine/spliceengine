package com.splicemachine.si.api;

/**
 * @author Scott Fines
 *         Created on: 8/21/13
 */
public interface RollForwardQueue<Data, Hashable extends Comparable> {

    void start();

    void stop();

    void recordRow(long transactionId, Data rowKey);
}
