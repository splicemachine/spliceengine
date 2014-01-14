package com.splicemachine.derby.utils;

import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * @author Scott Fines
 *         Created on: 9/18/13
 */
public class AttemptsExhaustedException extends DoNotRetryIOException{
    public AttemptsExhaustedException() {
    }

    public AttemptsExhaustedException(String message) {
        super(message);
    }

    public AttemptsExhaustedException(Throwable cause) {
        super(cause.getMessage(), cause);
    }

    public AttemptsExhaustedException(String message, Throwable cause) {
        super(message, cause);
    }
}
