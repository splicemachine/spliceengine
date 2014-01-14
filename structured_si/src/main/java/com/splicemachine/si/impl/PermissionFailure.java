package com.splicemachine.si.impl;

import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * Indicates that a transaction does not have permission to write to a given table.
 */
public class PermissionFailure extends DoNotRetryIOException {
    /**
     * Used for serialization, DO NOT USE
     */
    @Deprecated
    public PermissionFailure() { super(); }
    public PermissionFailure(String message) { super(message); }
}
