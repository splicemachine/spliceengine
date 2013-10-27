package com.splicemachine.hbase.table;

import java.io.IOException;

/**
 * Thrown to indicate that a coprocessor exec was submitted to an incorrect region.
 *
 * @author Scott Fines
 * Created on: 10/27/13
 */
public class IncorrectRegionException extends IOException {
    public IncorrectRegionException() {
    }

    public IncorrectRegionException(String message) {
        super(message);
    }

    public IncorrectRegionException(String message, Throwable cause) {
        super(message, cause);
    }

    public IncorrectRegionException(Throwable cause) {
        super(cause);
    }
}
