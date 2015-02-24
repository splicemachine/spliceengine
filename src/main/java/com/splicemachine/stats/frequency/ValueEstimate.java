package com.splicemachine.stats.frequency;

import com.google.common.primitives.Longs;

import java.util.Comparator;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
class ValueEstimate<T> implements FrequencyEstimate<T>,Comparable<FrequencyEstimate<T>> {
    private T value;
    private long count;
    private long epsilon;
    private Comparator<? super T> comparator;

    public ValueEstimate(T v, long c, long eps, Comparator<? super T> comparator) {
        this.value = v;
        this.count = c;
        this.epsilon = eps;
        this.comparator = comparator;
    }

    @Override public T getValue() { return value; }

    @Override
    public int compareTo(FrequencyEstimate<T> o) {
        int compare = comparator.compare(value, o.getValue());
        if(compare!=0) return compare;
        compare = Longs.compare(count, o.count());
        if(compare!=0) return compare;
        return Longs.compare(epsilon,o.error());
    }

    @Override public long count() { return count; }
    @Override public long error() { return epsilon; }

    @Override
    public FrequencyEstimate<T> merge(FrequencyEstimate<T> other) {
        this.count+=other.count();
        this.epsilon+=other.error();
        return this;
    }
}
