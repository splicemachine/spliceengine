package com.splicemachine.stats.frequency;

import com.google.common.primitives.Longs;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
class LongValueEstimate implements LongFrequencyEstimate {
    private long value;
    private long count;
    private long epsilon;

    public LongValueEstimate(long v, long c, long eps) {
        this.value = v;
        this.count = c;
        this.epsilon = eps;
    }

    @Override public long value() { return value; }

    @Override
    public int compareTo(LongFrequencyEstimate o) {
        int compare = Longs.compare(value, o.value());
        if(compare!=0) return compare;
        compare = Longs.compare(count, o.count());
        if(compare!=0) return compare;
        return Longs.compare(epsilon,o.error());
    }

    @Override public Long getValue() { return value; }
    @Override public long count() { return count; }
    @Override public long error() { return epsilon; }

    @Override
    public FrequencyEstimate<Long> merge(FrequencyEstimate<Long> other) {
        this.count+=other.count();
        this.epsilon+=other.error();
        return this;
    }

    @Override public String toString() { return "("+value+","+count+","+epsilon+")"; }

    @Override public int hashCode() { return Longs.hashCode(value); }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof FrequencyEstimate)) return false;
        if(obj instanceof LongFrequencyEstimate){
            return value==((LongFrequencyEstimate)obj).value();
        }else {
            Object ob = ((FrequencyEstimate) obj).getValue();
            return ob instanceof Long && (Long) ob == value;
        }
    }
}
