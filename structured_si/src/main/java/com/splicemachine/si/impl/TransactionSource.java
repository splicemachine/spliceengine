package com.splicemachine.si.impl;

import java.io.IOException;

/**
 * Allows a mechanism of loading transactions to be plugged-in (e.g. this allows the caller to wire in their own cache).
 */
public interface TransactionSource {
    Transaction getTransaction(long timestamp) throws IOException;
}
