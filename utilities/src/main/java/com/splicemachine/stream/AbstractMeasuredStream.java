package com.splicemachine.stream;

import com.splicemachine.metrics.Stats;

/**
 * @author Scott Fines
 *         Date: 12/22/14
 */
public abstract class AbstractMeasuredStream<T,V extends Stats> extends AbstractStream<T> implements MeasuredStream<T,V>  {

    @Override
    public <K> MeasuredStream<K,V> transform(Transformer<T, K> transformer) {
        return new MeasuredStreams.TransformingMeasuredStream<>(this,transformer);
    }

    @Override
    public MeasuredStream<T,V> filter(Predicate<T> predicate) {
        return new MeasuredStreams.FilteredMeasuredStream<>(this,predicate);
    }

    @Override
    public MeasuredStream<T,V> limit(long maxSize) {
        return new MeasuredStreams.LimitedStream<>(this,maxSize);
    }
}
