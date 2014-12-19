package com.splicemachine.stream;

/**
 * @author Scott Fines
 *         Date: 12/19/14
 */
public abstract class ForwardingStream<T> extends AbstractStream<T> {
    protected final Stream<T> delegate;

    public ForwardingStream(Stream<T> delegate) {
        this.delegate = delegate;
    }

    @Override public T next() throws StreamException { return delegate.next(); }
    @Override public void close() throws StreamException { delegate.close(); }
}
