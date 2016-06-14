package com.splicemachine.stats.frequency;

import com.google.common.primitives.Longs;
import com.google.common.primitives.Doubles;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
class DoubleValueEstimate implements DoubleFrequencyEstimate {
    private double value;
    private long count;
    private long epsilon;

    public DoubleValueEstimate(double v, long c, long eps) {
        this.value = v;
        this.count = c;
        this.epsilon = eps;
    }

    @Override public double value() { return value; }

    @Override
    public int compareTo(DoubleFrequencyEstimate o) {
        int compare = Doubles.compare(value, o.value());
        if(compare!=0) return compare;
        compare = Longs.compare(count, o.count());
        if(compare!=0) return compare;
        return Longs.compare(epsilon,o.error());
    }

    @Override public Double getValue() { return value; }
    @Override public long count() { return count; }
    @Override public long error() { return epsilon; }

    @Override
    public FrequencyEstimate<Double> merge(FrequencyEstimate<Double> other) {
        this.count+=other.count();
        this.epsilon+=other.error();
        return this;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof  FrequencyEstimate)) return false;
        if(obj instanceof DoubleFrequencyEstimate)
            return ((DoubleFrequencyEstimate)obj).value()==value;
        Object ob = ((FrequencyEstimate)obj).getValue();
        return ob instanceof Double && (Double)ob == value;
    }

    @Override public String toString() { return "("+value+","+count+","+epsilon+")"; }
}
