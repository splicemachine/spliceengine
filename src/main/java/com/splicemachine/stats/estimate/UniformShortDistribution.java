package com.splicemachine.stats.estimate;

import com.splicemachine.stats.ShortColumnStatistics;
import com.splicemachine.stats.frequency.ShortFrequencyEstimate;
import com.splicemachine.stats.frequency.ShortFrequentElements;
import com.splicemachine.utils.ComparableComparator;

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 3/5/15
 */
public class UniformShortDistribution extends UniformDistribution<Short> implements ShortDistribution {
    private final double a;
    private final double b;

    public UniformShortDistribution(ShortColumnStatistics columnStats) {
        super(columnStats, ComparableComparator.<Short>newComparator());

        /*
         * The CumulativeDistributionFunction(CDF) is a line from (min,minCount) to (max,nonNullCount),
         * but there are edges cases:
         *
         * 1. Empty distribution--when there are no elements in the distribution,
         * 2. Distribution consisting of a single element
         * 3. distribution containing multiple elements.
         *
         * In situation 1 and 2, the slope is undefinied (since you have 0/0). In shear correctness
         * terms, we have checks elsewhere in the function that will handle these scenarios gracefully without
         * recourse to using the linear interpolation. However, we put checks here for clarity and extra
         * safety in case the code changes in the future (and also so that we can put this note somewhere)
         */
        if(columnStats.nonNullCount()==0){
            //the distribution is empty, so the CDF is the 0 function
            this.a = this.b = 0d;
        }else if(columnStats.max()==columnStats.min()){
            //the distribution contains a single element, so the CDF is the constant function
            this.a = 0d;
            this.b = columnStats.minValue();
        }else{
            /*
             * The uniform CDF is a constant line from (min,minCount) to (max,nonNullCount)
             */
            double at=columnStats.nonNullCount()-columnStats.minCount();
            at/=(columnStats.max()-columnStats.min());

            this.a=at;
            this.b=columnStats.nonNullCount()-a*columnStats.max();
        }
    }

    @Override
    protected long estimateRange(Short start, Short stop, boolean includeStart, boolean includeStop, boolean isMin) {
        return rangeSelectivity(start,stop,includeStart,includeStop,isMin);
    }

    @Override
    public long selectivity(short value) {
        ShortColumnStatistics scs = (ShortColumnStatistics)columnStats;
        if(value<scs.min()||value>scs.max()) return  0l;
        if(value==scs.min()) return scs.minCount();

        ShortFrequentElements fes = (ShortFrequentElements)scs.topK();
        ShortFrequencyEstimate shortFrequencyEstimate = fes.countEqual(value);
        if(shortFrequencyEstimate.count()>0) return shortFrequencyEstimate.count();
        return uniformEstimate();
    }

    @Override
    public long selectivityBefore(short stop, boolean includeStop) {
        ShortColumnStatistics scs = (ShortColumnStatistics)columnStats;
        if(stop<scs.min()||(!includeStop && stop==scs.min())) return 0l;

        return rangeSelectivity(scs.min(),stop,true,includeStop);
    }

    @Override
    public long selectivityAfter(short start, boolean includeStart) {
        ShortColumnStatistics scs = (ShortColumnStatistics)columnStats;
        if(start>scs.max()||(!includeStart && start==scs.max())) return 0l;
        return rangeSelectivity(start,scs.max(),includeStart,true);
    }

    @Override
    public long rangeSelectivity(short start, short stop, boolean includeStart, boolean includeStop) {
        ShortColumnStatistics scs = (ShortColumnStatistics)columnStats;
        if(start==stop &&(!includeStart || !includeStop)) return 0l; //asking for an empty range
        short min = scs.min();
        if(stop<min||(!includeStop && stop==min)) return 0l;
        else if(includeStop && stop==min) return selectivity(min);

        short max = scs.max();
        if(start>max||(!includeStart && start==max)) return 0l;
        else if(includeStart && start==max) return selectivity(max);

        /*
         * We now have a range [a,b) which definitely overlaps, but it might not be wholly contained. Adjust
         * it to fit wholly within the data range
         */
        boolean isMin = false;
        if(start<=min){
            includeStart=includeStart||start<min;
            start = min;
            isMin = true;
        }
        if(stop>max) {
            stop = max;
            includeStop= true;
        }
        return rangeSelectivity(start,stop,includeStart,includeStop,isMin);
    }

    public long cardinalityBefore(short stop,boolean includeStop){
        ShortColumnStatistics scs = (ShortColumnStatistics)columnStats;
        if(stop<scs.min()||(!includeStop && stop==scs.min())) return 0l;

        return rangeCardinality(scs.min(),stop,true,includeStop);
    }

    public long cardinalityAfter(short start,boolean includeStart){
        ShortColumnStatistics scs = (ShortColumnStatistics)columnStats;
        if(start>scs.max()||(!includeStart && start==scs.max())) return 0l;
        return rangeCardinality(start,scs.max(),includeStart,true);
    }

    public long rangeCardinality(short start,short stop,boolean includeStart,boolean includeStop){
        ShortColumnStatistics scs = (ShortColumnStatistics)columnStats;
        if(start==stop &&(!includeStart || !includeStop)) return 0l; //asking for an empty range
        short min = scs.min();
        if(stop<min||(!includeStop && stop==min)) return 0l;
        else if(includeStop && stop==min) return selectivity(min);

        short max = scs.max();
        if(start>max||(!includeStart && start==max)) return 0l;
        else if(includeStart && start==max) return selectivity(max);

        /*
         * We now have a range [a,b) which definitely overlaps, but it might not be wholly contained. Adjust
         * it to fit wholly within the data range
         */
        if(start<=min){
            includeStart=includeStart||start<min;
            start = min;
        }
        if(stop>max) {
            stop = max;
            includeStop= true;
        }

        int d = (stop-start);
        if(includeStop)d++;
        if(!includeStart)d--;
        return d;
    }

    public long cardinality(Short start,Short stop,boolean includeStart,boolean includeStop){
        if(start==null){
            if(stop==null) return cardinality();
            else return cardinalityBefore(stop,includeStop);
        }else if(stop==null) return cardinalityAfter(start,includeStart);
        else{
            return rangeCardinality(start,stop,includeStart,includeStop);
        }
    }

    @Override public Short minValue(){ return min(); }
    @Override public long minCount(){ return columnStats.minCount(); }
    @Override public Short maxValue(){ return max(); }
    @Override public long totalCount(){ return columnStats.nonNullCount(); }
    public long cardinality(){ return columnStats.cardinality(); }
    @Override public short min(){ return ((ShortColumnStatistics)columnStats).min(); }
    @Override public short max(){ return ((ShortColumnStatistics)columnStats).max(); }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private long rangeSelectivity(short start, short stop, boolean includeStart, boolean includeStop, boolean isMin) {
        double baseEstimate = a*(stop-start);

        //adjust using Frequent Elements
        ShortFrequentElements sfe = (ShortFrequentElements)columnStats.topK();
        //if we are the min value, don't include the start key in frequent elements
        boolean includeMinFreqs = includeStart &&!isMin;
        Set<ShortFrequencyEstimate> shortFrequencyEstimates = sfe.frequentBetween(start, stop, includeMinFreqs, includeStop);
        long l=uniformRangeCount(includeMinFreqs,includeStop,baseEstimate,shortFrequencyEstimates);
        if(includeStart && isMin)
            l+=minCount();
        return l;
    }
}
