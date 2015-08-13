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
public class UniformDoubleDistribution extends UniformDistribution<Double> implements DoubleDistribution {
    private final double a;
    private final double b;
    public UniformDoubleDistribution(DoubleColumnStatistics columnStats) {
        super(columnStats, ComparableComparator.<Double>newComparator());
        if(columnStats.nonNullCount()==0){
            /*
             * the distribution is empty, so our interpolation function is the 0 function
             */
            this.a = 0d;
            this.b = 0d;
        }else if(columnStats.max()==columnStats.min()){
            /*
             * The distribution contains only a single element, so our interpolation is the constant function
             */
            this.a = 0d;
            this.b = columnStats.minCount();
        }else{
            /*
             * Create a linear interpolator to estimate to the Cumulative probability function of a uniform
             * distribution
             */
            double at=columnStats.nonNullCount()-columnStats.minCount();
            at/=(columnStats.max()-columnStats.min());

            this.a=at;
            this.b=columnStats.nonNullCount()-a*columnStats.max();
        }
    }

    public long cardinalityBefore(double stop,boolean includeStop){
        DoubleColumnStatistics fcs = (DoubleColumnStatistics)columnStats;
        double min = fcs.min();
        if(stop<min ||(!includeStop && min==stop)) return 0l;

        return rangeCardinality(min,stop,true,includeStop);
    }

    public long cardinalityAfter(double start,boolean includeStart){
        DoubleColumnStatistics fcs = (DoubleColumnStatistics)columnStats;
        double max = fcs.max();
        if(start>max || (!includeStart &&start==max)) return 0l;

        return rangeSelectivity(start,max,includeStart,true);
    }

    public long rangeCardinality(double start,double stop,boolean includeStart,boolean includeStop){
        DoubleColumnStatistics fcs = (DoubleColumnStatistics)columnStats;
        double min = fcs.min();
        if(stop<min||(!includeStop && stop==min)) return 0l;
        else if(includeStop && stop==min) return selectivity(stop);

        double max = fcs.max();
        if(max<start||(!includeStart && start==max)) return 0l;
        else if(includeStart && start==min) return selectivity(start);

        if(start<=min){
            includeStart = includeStart||start<min;
            start = min;
        }
        if(stop>max){
            stop = max;
            includeStop = true;
        }

        double diff = (stop-start);
        if(includeStop)diff++;
        if(!includeStart) diff--;
        return (long)diff;
    }

    public long cardinality(Double start,Double stop,boolean includeStart,boolean includeStop){
        if(start==null){
            if(stop==null) return cardinality();
            else return cardinalityBefore(stop,includeStop);
        }else if(stop==null) return cardinalityAfter(start,includeStart);
        else return rangeCardinality(start,stop,includeStart,includeStop);
    }

    @Override public double min(){ return ((DoubleColumnStatistics)columnStats).min(); }
    @Override public double max(){ return ((DoubleColumnStatistics)columnStats).max(); }
    @Override public Double minValue(){ return min(); }
    @Override public Double maxValue(){ return max(); }
    @Override public long totalCount(){ return columnStats.nonNullCount(); }
    @Override public long minCount(){ return columnStats.minCount(); }
    public long cardinality(){ return columnStats.cardinality(); }

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
        return uniformEstimate();
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
        double baseEstimate = a*(stop-start);

        DoubleFrequentElements ife = (DoubleFrequentElements)columnStats.topK();
        //if we are the min value, don't include the start key in frequent elements
        includeStart = includeStart &&!isMin;
        Set<DoubleFrequencyEstimate> ffe = ife.frequentBetween(start, stop, includeStart, includeStop);
        return uniformRangeCount(includeStart,includeStop,baseEstimate,ffe);
    }

}
