package com.splicemachine.si.impl;

import java.io.IOException;
import java.util.List;

/**
 * Abstraction over the details of what it means to roll-forward a transaction. This allows the RollForwardQueue to be
 * decoupled.
 */
public interface RollForwardAction<Data> {
    Boolean rollForward(long transactionId, List<Data> rowList) throws IOException;
}
