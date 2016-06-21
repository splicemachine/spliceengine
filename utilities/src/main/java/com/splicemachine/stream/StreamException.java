package com.splicemachine.stream;

/**
 * Exception thrown when something goes wrong during the processing
 * of a Stream.
 *
 * This is often used as a wrapper around a deeper underlying exception (much like
 * {@link java.util.concurrent.ExecutionException}).
 *
 * @author Scott Fines
 * Date: 8/13/14
 */
public class StreamException extends Exception {

    public StreamException() { }

    public StreamException(String message) { super(message); }

    public StreamException(String message, Throwable cause) { super(message, cause); }

    public StreamException(Throwable cause) { super(cause); }
}
