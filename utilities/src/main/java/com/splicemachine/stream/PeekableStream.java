package com.splicemachine.stream;

/**
 * @author Scott Fines
 * Date: 8/13/14
 */
public interface PeekableStream<T> extends Stream<T> {

    T peek() throws StreamException;

    /**
     * Called when you want to remove the peeked element from the Stream. This means that
     *
     * {@code
     *  T n = stream.peek();
     *  stream.take();
     * }
     *
     * is functionally equivalent to calling
     * {@code
     * T n = stream.peek();
     * n = stream.next();
     * }
     * But the error handling is easier.
     */
    void take();
}
