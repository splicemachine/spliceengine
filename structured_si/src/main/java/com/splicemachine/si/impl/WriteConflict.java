package com.splicemachine.si.impl;

import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * Exception indicating that a transaction failed because it tried to write to data that was modified since the
 * transaction began (i.e. the transaction collided with another).
 */
public class WriteConflict extends DoNotRetryIOException {
    /**
     * Used for serialization, DO NOT USE
     */
    @Deprecated
    public WriteConflict() { super(); }
    public WriteConflict(String message) { super(message); }
    public WriteConflict(String message, Throwable cause) { super(message, cause);}
}
