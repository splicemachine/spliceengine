package com.splicemachine.derby.hbase.job;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public enum  Status {
    PENDING,
    EXECUTING,
    FAILED,
    COMPLETED,
    CANCELLED
}
