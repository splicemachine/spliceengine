package com.splicemachine.stats.estimate;

import com.splicemachine.stats.DoubleColumnStatistics;
import com.splicemachine.stats.frequency.DoubleFrequencyEstimate;
import com.splicemachine.stats.frequency.DoubleFrequentElements;
import com.splicemachine.utils.ComparableComparator;

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 3/5/15
 */
public class UniformDoubleDistribution extends BaseDistribution<Double> implements DoubleDistribution {

    public UniformDoubleDistribution(DoubleColumnStatistics columnStats) {
        super(columnStats, ComparableComparator.<Double>newComparator());
    }

    @Override
    protected long estimateRange(Double start, Double stop, boolean includeStart, boolean includeStop, boolean isMin) {
        return rangeSelectivity(start,stop,includeStart,includeStop,isMin);
    }

    @Override
    public long selectivity(double value) {
        DoubleColumnStatistics fcs = (DoubleColumnStatistics)columnStats;
        if(value<fcs.min()) return 0l;
        else if(value==fcs.min()) return fcs.minCount();
        else if(value>fcs.max()) return 0l;

        DoubleFrequentElements ffe = (DoubleFrequentElements)fcs.topK();
        DoubleFrequencyEstimate doubleFrequencyEstimate = ffe.countEqual(value);
        if(doubleFrequencyEstimate.count()>0) return doubleFrequencyEstimate.count();

        //not a frequent element, so estimate the value using cardinality and adjusted row counts
        return getAdjustedRowCount()/fcs.cardinality();
    }

    @Override
    public long selectivityBefore(double stop, boolean includeStop) {
        DoubleColumnStatistics fcs = (DoubleColumnStatistics)columnStats;
        double min = fcs.min();
        if(stop<min ||(!includeStop && min==stop)) return 0l;

        return rangeSelectivity(min,stop,true,includeStop);
    }

    @Override
    public long selectivityAfter(double start, boolean includeStart) {
        DoubleColumnStatistics fcs = (DoubleColumnStatistics)columnStats;
        double max = fcs.max();
        if(start>max || (!includeStart &&start==max)) return 0l;

        return rangeSelectivity(start,max,includeStart,true);
    }

    @Override
    public long rangeSelectivity(double start, double stop, boolean includeStart, boolean includeStop) {
        DoubleColumnStatistics fcs = (DoubleColumnStatistics)columnStats;
        double min = fcs.min();
        if(stop<min||(!includeStop && stop==min)) return 0l;
        else if(includeStop && stop==min) return selectivity(stop);

        double max = fcs.max();
        if(max<start||(!includeStart && start==max)) return 0l;
        else if(includeStart && start==min) return selectivity(start);

        boolean isMin = false;
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
    private long rangeSelectivity(double start, double stop, boolean includeStart, boolean includeStop,boolean isMin) {
        long perEntryCount = getAdjustedRowCount()/columnStats.cardinality();
        long baseEstimate = (long)(perEntryCount*(stop-start));
        if(!includeStart){
            baseEstimate-=perEntryCount;
        }else if(isMin){
            baseEstimate-=perEntryCount;
            baseEstimate+=columnStats.minCount();
        }
        if(includeStop)
            baseEstimate+=perEntryCount;

        DoubleFrequentElements ife = (DoubleFrequentElements)columnStats.topK();
        //if we are the min value, don't include the start key in frequent elements
        includeStart = includeStart &&!isMin;
        Set<DoubleFrequencyEstimate> ffe = ife.frequentBetween(start, stop, includeStart, includeStop);
        baseEstimate-=perEntryCount*ffe.size();
        for(DoubleFrequencyEstimate est:ffe){
            baseEstimate+=est.count();
        }
        return baseEstimate;
    }
}
