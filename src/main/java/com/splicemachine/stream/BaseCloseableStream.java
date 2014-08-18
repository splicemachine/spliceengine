package com.splicemachine.stream;

/**
 * @author Scott Fines
 * Date: 8/13/14
 */
public abstract class BaseCloseableStream<T> implements CloseableStream<T>{
    @Override
    public <R> CloseableStream<R> transform(Transformer<T, R> transformer) {
        return new TransformingCloseableStream<T,R>(this,transformer);
    }

    @Override
    public void forEach(Accumulator<T> accumulator) throws StreamException {
        T n;
        while((n = next())!=null)
            accumulator.accumulate(n);
    }
}
