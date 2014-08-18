package com.splicemachine.stream;

import java.io.Closeable;

/**
 * @author Scott Fines
 * Date: 8/13/14
 */
public interface CloseableStream<T> extends Stream<T>,Closeable {

    <R> CloseableStream<R> transform(Transformer<T,R> transformer);
}
