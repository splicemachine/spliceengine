package com.splicemachine.stream;


import com.splicemachine.metrics.Stats;

/**
 * @author Scott Fines
 *         Date: 12/19/14
 */
public interface MeasuredStream<T,V extends Stats> extends Stream<T> {

    /**
     * @return stats collected <em>as of this point in time</em>. If the stream has not been
     * exhausted, then calling {@code next()} may change the value of the returned stats. This change
     * may or may not be reflected in the stats returned, depending on the implementation.
     */
    V getStats();

    @Override
    <R> MeasuredStream<R,V> transform(Transformer<T, R> transformer);

    @Override
    MeasuredStream<T,V> filter(Predicate<T> predicate);

    @Override
    MeasuredStream<T,V> limit(long maxSize);
}
