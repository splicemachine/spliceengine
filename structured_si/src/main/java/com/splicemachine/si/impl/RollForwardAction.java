package com.splicemachine.si.impl;

import java.io.IOException;

/**
 * Abstraction over the details of what it means to roll-forward a transaction. This allows the RollForwardQueue to be
 * decoupled.
 */
public interface RollForwardAction {
    Boolean rollForward(long transactionId, Long effectiveTimestamp, byte[] rowKey) throws IOException;
}
