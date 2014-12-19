package com.splicemachine.stream;

import com.splicemachine.metrics.Stats;

/**
 * @author Scott Fines
 *         Date: 12/22/14
 */
public class TransformingMeasuredStream<E,R,V extends Stats> extends AbstractMeasuredStream<R,V>{
    private final MeasuredStream<E,V> delegate;
    private final Transformer<E, R> transformer;

    public TransformingMeasuredStream(MeasuredStream<E, V> delegate, Transformer<E, R> transformer) {
        this.delegate = delegate;
        this.transformer = transformer;
    }

    @Override
    public <K> MeasuredStream<K,V> transform(Transformer<R, K> transformer) {
        return new TransformingMeasuredStream<>(this,transformer);
    }

    @Override
    public R next() throws StreamException {
        E n = delegate.next();
        if(n==null) return null;
        return transformer.transform(n);
    }

    @Override public void close() throws StreamException { delegate.close(); }

    @Override
    public V getStats() {
        return delegate.getStats();
    }
}
