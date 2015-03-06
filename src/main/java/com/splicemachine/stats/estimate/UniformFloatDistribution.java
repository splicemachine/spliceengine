package com.splicemachine.stats.estimate;

import com.splicemachine.stats.FloatColumnStatistics;
import com.splicemachine.stats.frequency.FloatFrequencyEstimate;
import com.splicemachine.stats.frequency.FloatFrequentElements;
import com.splicemachine.utils.ComparableComparator;

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 3/5/15
 */
public class UniformFloatDistribution extends BaseDistribution<Float> implements FloatDistribution {

    public UniformFloatDistribution(FloatColumnStatistics columnStats) {
        super(columnStats, ComparableComparator.<Float>newComparator());
    }

    @Override
    protected long estimateRange(Float start, Float stop, boolean includeStart, boolean includeStop, boolean isMin) {
        return rangeSelectivity(start,stop,includeStart,includeStop,isMin);
    }

    @Override
    public long selectivity(float value) {
        FloatColumnStatistics fcs = (FloatColumnStatistics)columnStats;
        if(value<fcs.min()) return 0l;
        else if(value==fcs.min()) return fcs.minCount();
        else if(value>fcs.max()) return 0l;

        FloatFrequentElements ffe = (FloatFrequentElements)fcs.topK();
        FloatFrequencyEstimate floatFrequencyEstimate = ffe.countEqual(value);
        if(floatFrequencyEstimate.count()>0) return floatFrequencyEstimate.count();

        //not a frequent element, so estimate the value using cardinality and adjusted row counts
        return getAdjustedRowCount()/fcs.cardinality();
    }

    @Override
    public long rangeSelectivity(float start, float stop, boolean includeStart, boolean includeStop) {
        FloatColumnStatistics fcs = (FloatColumnStatistics)columnStats;
        float min = fcs.min();
        if(stop<min||(!includeStop && stop==min)) return 0l;
        else if(includeStop && stop==min) return selectivity(stop);

        float max = fcs.max();
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
    private long rangeSelectivity(float start, float stop, boolean includeStart, boolean includeStop,boolean isMin) {
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

        FloatFrequentElements ife = (FloatFrequentElements)columnStats.topK();
        Set<FloatFrequencyEstimate> ffe = ife.frequentBetween(start, stop, includeStart, includeStop);
        baseEstimate-=perEntryCount*ffe.size();
        for(FloatFrequencyEstimate est:ffe){
            baseEstimate+=est.count();
        }
        return baseEstimate;
    }
}
