package com.splicemachine.stats.frequency;

import com.google.common.primitives.Longs;
import com.google.common.primitives.Floats;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
class FloatValueEstimate implements FloatFrequencyEstimate {
    private float value;
    private long count;
    private long epsilon;

    public FloatValueEstimate(float v, long c, long eps) {
        this.value = v;
        this.count = c;
        this.epsilon = eps;
    }

    @Override public float value() { return value; }

    @Override
    public int compareTo(FloatFrequencyEstimate o) {
        int compare = Floats.compare(value, o.value());
        if(compare!=0) return compare;
        compare = Longs.compare(count, o.count());
        if(compare!=0) return compare;
        return Longs.compare(epsilon,o.error());
    }

    @Override public Float getValue() { return value; }
    @Override public long count() { return count; }
    @Override public long error() { return epsilon; }

    @Override
    public FrequencyEstimate<Float> merge(FrequencyEstimate<Float> other) {
        this.count+=other.count();
        this.epsilon+=other.error();
        return this;
    }
}
