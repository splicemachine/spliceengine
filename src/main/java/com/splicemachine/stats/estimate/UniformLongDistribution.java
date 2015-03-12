package com.splicemachine.stats.estimate;

import com.splicemachine.stats.LongColumnStatistics;
import com.splicemachine.stats.frequency.LongFrequencyEstimate;
import com.splicemachine.stats.frequency.LongFrequentElements;
import com.splicemachine.utils.ComparableComparator;

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 3/5/15
 */
public class UniformLongDistribution extends BaseDistribution<Long> implements LongDistribution {
    public UniformLongDistribution(LongColumnStatistics columnStats) {
        super(columnStats, ComparableComparator.<Long>newComparator());
    }

    @Override
    protected long estimateRange(Long start, Long stop, boolean includeStart, boolean includeStop, boolean isMin) {
        return rangeSelectivity(start,stop,includeStart,includeStop,isMin);
    }

    @Override
    public long selectivity(long value) {
        LongColumnStatistics scs = (LongColumnStatistics)columnStats;
        if(value<scs.min()||value>scs.max()) return  0l;
        if(value==scs.min()) return scs.minCount();

        LongFrequentElements fes = (LongFrequentElements)scs.topK();
        LongFrequencyEstimate longFrequencyEstimate = fes.countEqual(value);
        if(longFrequencyEstimate.count()>0) return longFrequencyEstimate.count();
        return getPerRowCount();
    }

    @Override
    public long selectivityBefore(long stop, boolean includeStop) {
        LongColumnStatistics scs = (LongColumnStatistics)columnStats;
        if(stop<scs.min()||(!includeStop && stop==scs.min())) return 0l;

        return rangeSelectivity(scs.min(),stop,true,includeStop);
    }

    @Override
    public long selectivityAfter(long start, boolean includeStart) {
        LongColumnStatistics scs = (LongColumnStatistics)columnStats;
        if(start>scs.max()||(!includeStart && start==scs.max())) return 0l;

        return rangeSelectivity(start,scs.max(),includeStart,true);
    }

    @Override
    public long rangeSelectivity(long start, long stop, boolean includeStart, boolean includeStop) {
        LongColumnStatistics scs = (LongColumnStatistics)columnStats;
        if(start==stop &&(!includeStart || !includeStop)) return 0l; //asking for an empty range
        long min = scs.min();
        if(stop<min||(!includeStop && stop==min)) return 0l;
        else if(includeStop && stop==min) return selectivity(min);

        long max = scs.max();
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

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private long rangeSelectivity(long start, long stop, boolean includeStart, boolean includeStop, boolean isMin) {
        long perRowCount = getPerRowCount();
        long baseEstimate = perRowCount*(stop-start);
        if(!includeStart){
            baseEstimate-=perRowCount;
        }else if(isMin){
            baseEstimate-=perRowCount;
            baseEstimate+=columnStats.minCount();
        }

        if(includeStop)
            baseEstimate+=perRowCount;

        //adjust using Frequent Elements
        LongFrequentElements sfe = (LongFrequentElements)columnStats.topK();
        //if we are the min value, don't include the start key in frequent elements
        includeStart = includeStart &&!isMin;
        Set<LongFrequencyEstimate> longFrequencyEstimates = sfe.frequentBetween(start, stop, includeStart, includeStop);
        baseEstimate-=longFrequencyEstimates.size()*perRowCount;
        for(LongFrequencyEstimate est:longFrequencyEstimates){
            baseEstimate+=est.count();
        }
        return baseEstimate;
    }

    private long getPerRowCount() {
        return getAdjustedRowCount()/columnStats.cardinality();
    }
}
