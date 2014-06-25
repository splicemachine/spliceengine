package com.splicemachine.si.impl.timestamp;

import java.io.IOException;

/**
 * Thrown when something goes wrong fetching a new transaction timestamp
 * from the server.
 */
public class TimestampIOException extends IOException {

    public TimestampIOException() {
    }

    public TimestampIOException(String message) {
        super(message);
    }

    public TimestampIOException(String message, Throwable cause) {
        super(message, cause);
    }

    public TimestampIOException(Throwable cause) {
        super(cause);
    }
}
