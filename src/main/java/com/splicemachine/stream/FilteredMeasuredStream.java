package com.splicemachine.stream;

import com.splicemachine.metrics.Stats;

/**
 * @author Scott Fines
 *         Date: 2/5/15
 */
public class FilteredMeasuredStream<T,V extends Stats> extends AbstractMeasuredStream<T,V> {
    private final Predicate<T> predicate;
    private final MeasuredStream<T,V> delegate;
    public FilteredMeasuredStream(MeasuredStream<T,V> delegate, Predicate<T> predicate) {
        this.delegate = delegate;
        this.predicate = predicate;
    }

    @Override
    public V getStats() {
        return null;
    }

    @Override
    public T next() throws StreamException {
        T n;
        while((n = delegate.next())!=null){
            if(predicate.apply(n)) return n;
        }
        return null;
    }

    @Override public void close() throws StreamException { delegate.close(); }
}
