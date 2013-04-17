package com.splicemachine.si.impl;

import org.apache.hadoop.hbase.DoNotRetryIOException;

public class WriteConflict extends DoNotRetryIOException {
    /**
     * Used for serialization, DO NOT USE
     */
    @Deprecated
    public WriteConflict() { super(); }
    public WriteConflict(String message) { super(message); }
    public WriteConflict(String message, Throwable cause) { super(message, cause);}
}
