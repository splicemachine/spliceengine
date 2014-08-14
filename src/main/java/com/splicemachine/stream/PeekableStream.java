package com.splicemachine.stream;

/**
 * @author Scott Fines
 * Date: 8/13/14
 */
public interface PeekableStream<T> extends Stream<T> {

    T peek() throws StreamException;
}
