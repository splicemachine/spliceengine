package com.splicemachine.stream;

import com.splicemachine.metrics.Stats;

/**
 * @author Scott Fines
 *         Date: 2/12/15
 */
public class ForwardingMeasuredStream<T,V extends Stats> extends AbstractMeasuredStream<T,V> {
    protected MeasuredStream<T,V> delegate;

    public ForwardingMeasuredStream(MeasuredStream<T, V> delegate) { this.delegate = delegate; }

    @Override public V getStats() { return delegate.getStats(); }
    @Override public T next() throws StreamException { return delegate.next(); }
    @Override public void close() throws StreamException { delegate.close(); }
}
