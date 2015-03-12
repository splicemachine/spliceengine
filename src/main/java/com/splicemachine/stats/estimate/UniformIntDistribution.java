package com.splicemachine.stats.estimate;

import com.splicemachine.stats.IntColumnStatistics;
import com.splicemachine.stats.frequency.IntFrequencyEstimate;
import com.splicemachine.stats.frequency.IntFrequentElements;
import com.splicemachine.utils.ComparableComparator;

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 3/5/15
 */
public class UniformIntDistribution extends BaseDistribution<Integer> implements IntDistribution {
    public UniformIntDistribution(IntColumnStatistics columnStats) {
        super(columnStats, ComparableComparator.<Integer>newComparator());
    }

    @Override
    protected long estimateRange(Integer start, Integer stop, boolean includeStart, boolean includeStop, boolean isMin) {
        return rangeSelectivity(start,stop,includeStart,includeStop,isMin);
    }

    @Override
    public long selectivity(int value) {
        IntColumnStatistics ics = (IntColumnStatistics)columnStats;
        if(value<ics.min()) return 0l;
        else if(value==ics.min()) return ics.minCount();
        else if(value>ics.max()) return 0l;

        IntFrequencyEstimate est = ((IntFrequentElements)ics.topK()).countEqual(value);
        if(est.count()>0) return est.count();
        else return getAdjustedRowCount()/columnStats.cardinality();
    }

    @Override
    public long selectivityBefore(int stop, boolean includeStop) {
        IntColumnStatistics ics = (IntColumnStatistics)columnStats;
        if(stop<ics.min()||(!includeStop && stop==ics.min())) return 0l;

        return rangeSelectivity(ics.min(),stop,true,includeStop);
    }

    @Override
    public long selectivityAfter(int start, boolean includeStart) {
        IntColumnStatistics ics = (IntColumnStatistics)columnStats;
        if(start>ics.max()||(!includeStart && start==ics.max())) return 0l;

        return rangeSelectivity(start,ics.max(),includeStart,true);
    }

    @Override
    public long rangeSelectivity(int start, int stop, boolean includeStart, boolean includeStop) {
        if(start==stop &&(!includeStart || !includeStop)) return 0l; //empty interval has no data
        IntColumnStatistics ics = (IntColumnStatistics)columnStats;
        int min = ics.min();
        if(min>stop||(!includeStop && stop==min)) return 0l;
        else if(includeStop && stop==min) return selectivity(stop);

        int max = ics.max();
        if(max<start||(!includeStart && start==max)) return 0l;
        else if(includeStart && start==max) return selectivity(start);

        boolean isMin= false;
        if(start<=min){
            includeStart = includeStart||start<min;
            start = min;
            isMin=true;
        }
        if(stop>max){
            stop = max;
            includeStop = true;
        }
        return rangeSelectivity(start,stop,includeStart,includeStop,isMin);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private long rangeSelectivity(int start, int stop, boolean includeStart, boolean includeStop,boolean isMin) {

        long perRowCount = getAdjustedRowCount()/columnStats.cardinality();
        long baseEstimate = perRowCount*(start-stop);

        if(!includeStart){
            baseEstimate-= perRowCount;
        }else if(isMin){
            baseEstimate-=perRowCount;
            baseEstimate+=columnStats.minCount();
        }
        if(includeStop)
            baseEstimate+=perRowCount;

        //adjust using Frequent Elements
        IntFrequentElements ife = (IntFrequentElements)columnStats.topK();
        //if we are the min value, don't include the start key in frequent elements
        includeStart = includeStart &&!isMin;
        Set<IntFrequencyEstimate> intFrequencyEstimates = ife.frequentBetween(start, stop, includeStart, includeStop);
        baseEstimate-=perRowCount*intFrequencyEstimates.size();
        for(IntFrequencyEstimate estimate: intFrequencyEstimates){
            baseEstimate+=estimate.count();
        }
        return baseEstimate;
    }
}
