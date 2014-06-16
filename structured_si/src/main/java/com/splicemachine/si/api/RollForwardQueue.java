package com.splicemachine.si.api;

import com.splicemachine.si.impl.RollForwardAction;

/**
 * @author Scott Fines
 * Created on: 8/21/13
 */
public interface RollForwardQueue {
    void start();
    void stop();
    void recordRow(long transactionId, byte[] rowKey, Long effectiveTimestamp);
    int getCount();
}
