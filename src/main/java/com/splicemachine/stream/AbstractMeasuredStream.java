package com.splicemachine.stream;

import com.splicemachine.metrics.Stats;

/**
 * @author Scott Fines
 *         Date: 12/22/14
 */
public abstract class AbstractMeasuredStream<T,V extends Stats> extends AbstractStream<T> implements MeasuredStream<T,V>  {

    @Override
    public <K> MeasuredStream<K,V> transform(Transformer<T, K> transformer) {
        return new TransformingMeasuredStream<>(this,transformer);
    }

    @Override
    public MeasuredStream<T,V> filter(Predicate<T> predicate) {
        return new FilteredMeasuredStream<>(this,predicate);
    }
}
