package com.splicemachine.stream;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 8/13/14
 */
class TransformingCloseableStream<T, R> extends BaseCloseableStream<R> {
    private final CloseableStream<T> stream;
    private final Transformer<T, R> transformer;

    public TransformingCloseableStream(CloseableStream<T> stream, Transformer<T,R> transformer) {
        this.stream = stream;
        this.transformer = transformer;
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }

    @Override
    public R next() throws StreamException {
        T n = stream.next();
        if(n==null) return null;
        return transformer.transform(n);
    }
}
