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
public class UniformShortDistribution extends BaseDistribution<Short> implements ShortDistribution {
    private final double a;
    private final double b;

    public UniformShortDistribution(ShortColumnStatistics columnStats) {
        super(columnStats, ComparableComparator.<Short>newComparator());

        double at = columnStats.nonNullCount()-columnStats.minCount();
        at/=(columnStats.max()-columnStats.min());

        this.a = at;
        this.b = columnStats.nonNullCount()-a*columnStats.max();
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
        return getPerRowCount();
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

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private long rangeSelectivity(short start, short stop, boolean includeStart, boolean includeStop, boolean isMin) {
        double baseEstimate = a*stop+b;
        baseEstimate-=a*start+b;
        long perRowCount = getPerRowCount();
        if(!includeStart){
            baseEstimate-=perRowCount;
        }
        if(includeStop)
            baseEstimate+=perRowCount;

        //adjust using Frequent Elements
        ShortFrequentElements sfe = (ShortFrequentElements)columnStats.topK();
        //if we are the min value, don't include the start key in frequent elements
        includeStart = includeStart &&!isMin;
        Set<ShortFrequencyEstimate> shortFrequencyEstimates = sfe.frequentBetween(start, stop, includeStart, includeStop);
        baseEstimate-=shortFrequencyEstimates.size()*perRowCount;
        for(ShortFrequencyEstimate est:shortFrequencyEstimates){
            baseEstimate+=est.count();
        }
        return (long)baseEstimate;
    }

    private long getPerRowCount() {
        return getAdjustedRowCount()/columnStats.cardinality();
    }
}
