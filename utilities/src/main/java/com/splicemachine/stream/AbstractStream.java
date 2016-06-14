package com.splicemachine.stream;

import java.util.Iterator;

/**
 * Skeleton implementation of the Stream interface
 * @author Scott Fines
 * Date: 8/13/14
 */
public abstract class AbstractStream<T> implements Stream<T> {

    @Override
    public <V> Stream<V> transform(Transformer<T, V> transformer) {
        return new TransformingStream<>(this,transformer);
    }

    @Override
    public void forEach(Accumulator<T> accumulator) throws StreamException {
        T n;
        while((n = next())!=null){
            accumulator.accumulate(n);
        }
    }

    @Override public Stream<T> filter(Predicate<T> predicate) { return new Streams.FilteredStream<>(this,predicate); }
    @Override public Iterator<T> asIterator() { return new StreamIterator<>(this); }
    @Override public Stream<T> limit(long maxSize) { return new Streams.LimitedStream<>(this,maxSize);}
}
