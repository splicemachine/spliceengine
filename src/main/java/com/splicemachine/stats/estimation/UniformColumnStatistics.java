package com.splicemachine.stats.estimation;

import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;

import java.util.Set;

/**
 * A column statistics view that makes the "uniform distribution" assumption.
 *
 * This assumption is generally pretty bad, because most real-world distributions don't reflect
 * this. That makes this primarily useful as a mechanism for estimating values without reference
 * to a histogram(which may be complex and/or too expensive to construct).
 *
 * @author Scott Fines
 *         Date: 12/5/14
 */
public abstract class UniformColumnStatistics<T extends Comparable<T>> implements OrderedColumnStatistics<T> {

    private final GlobalColumnStatistics<T> columnStats;
    private final T min;
    private final long minCount;
    private final T max;
    private final long maxCount;

    public UniformColumnStatistics(GlobalColumnStatistics<T> columnStats,
                                   T min,
                                   long minCount, T max, long maxCount) {
        this.columnStats = columnStats;
        this.min = min;
        this.minCount = minCount;
        this.max = max;
        this.maxCount = maxCount;
    }

    @Override public T minValue() { return min; }
    @Override public long minCount() { return minCount; }
    @Override public T maxValue() { return max; }
    @Override public long maxCount() { return maxCount; }
    @Override public long nullCount() { return columnStats.nullCount(); }
    @Override public long nonNullCount() { return columnStats.nonNullCount(); }
    @Override public FrequentElements<T> mostFrequentElements() { return columnStats.mostFrequentElements(); }
    @Override public long cardinality() { return columnStats.cardinality(); }
    @Override public float duplicateFactor() { return columnStats.duplicateFactor(); }
    @Override public int averageSize() { return columnStats.averageSize(); }

    @Override
    public long between(T start, T stop, boolean includeMin, boolean includeMax) {
        /*
         * The number of elements which fall in the range [a,b) is the sum of equals
         * for all elements between a and b. Since elements are uniform, we assume that
         * they all have the same number. Thus, we estimate equals(start), then multiply
         * that by (b-a) (and then deal with end points)
         */
        if(start==null){
            if(stop==null) return nullCount()+nonNullCount(); //everything
            start =min;
            includeMin=true;
        }

        if(stop==null){
            stop=max;
            includeMax=true;
        }
        if(stop.equals(start)){
            if(includeMax) return equals(start);
            else return 0l;
        }

        long numBetween = distance(start,stop);
        if(!includeMin)
            numBetween--;
        if(includeMax)
            numBetween++;

        /*
         * Use frequent elements to adjust the values that we have
         */
        long count = 0l;
        Set<? extends FrequencyEstimate<T>> containedElements = mostFrequentElements().frequentElementsBetween(start,stop,includeMin,includeMax);
        for(FrequencyEstimate<T> estimate: containedElements){
            count+=estimate.count();
            numBetween--;
        }
        count+=numBetween*nonNullCount()/cardinality();

        return count;
    }

    /**
     * Get the number of elements which fit in the interval [start,stop) (e.g.
     * the equivalent of |stop-start|.
     *
     * @param start the start of the interval
     * @param stop the end of the interval
     * @return the distance between start and stop.
     */
    protected abstract long distance(T start, T stop);

    @Override
    public long equals(T value) {
        /*
         * the mechanism for estimation is relatively straightforward here. We assume
         * all data elements are uniformly distributed, with #cardinality() unique elements
         * in the entire data set.
         *
         * Therefore, the number of elements which are equal to what we want is
         * the duplicationFactor(nonNullCount/cardinality).
         */
        if(value==null) return nullCount();
        else if(value.equals(min)) return minCount;
        else if(value.equals(max)) return maxCount;
        else {
            long count = mostFrequentElements().equal(value).count();
            if(count!=0) return count;
            return nonNullCount()/cardinality();
        }
    }
}
