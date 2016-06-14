package com.splicemachine.stream;

/**
 * @author Scott Fines
 *         Date: 8/13/14
 */
class TransformingStream<T,V> extends AbstractStream<V> {
    private final Stream<T> stream;
    private final Transformer<T, V> transformer;

    TransformingStream(Stream<T> stream, Transformer<T, V> transformer) {
        this.stream = stream;
        this.transformer = transformer;
    }

    @Override
    public V next() throws StreamException {
        T n = stream.next();
        if(n==null) return null;
        return transformer.transform(n);
    }

    @Override
    public void close() throws StreamException  {stream.close(); }
}
