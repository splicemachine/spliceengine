package com.splicemachine.stream;

/**
 * Skeleton implementation of the Stream interface
 * @author Scott Fines
 * Date: 8/13/14
 */
public abstract class BaseStream<T> implements Stream<T> {

    @Override
    public <V> Stream<V> transform(Transformer<T, V> transformer) {
        return new TransformingStream<T,V>(this,transformer);
    }

    @Override
    public void forEach(Accumulator<T> accumulator) throws StreamException {
        T n;
        while((n = next())!=null){
            accumulator.accumulate(n);
        }
    }
}
