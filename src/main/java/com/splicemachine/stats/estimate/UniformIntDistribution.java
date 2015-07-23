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
public class UniformIntDistribution extends UniformDistribution<Integer> implements IntDistribution {
    private final double a;
    private final double b;

    public UniformIntDistribution(IntColumnStatistics columnStats) {
        super(columnStats, ComparableComparator.<Integer>newComparator());

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
            this.a = 0d;
            this.b = 0d;
        }
        else if(columnStats.max()==columnStats.min()||columnStats.nonNullCount()==0){
            //the dist. contains only a single element, so the CDF is a constant
            this.a = 0;
            this.b = columnStats.minCount();
        }else{
            //base case
            double at=columnStats.nonNullCount()-columnStats.minCount();
            at/=(columnStats.max()-columnStats.min());

            this.a=at;
            this.b=columnStats.nonNullCount()-a*columnStats.max();
        }
    }

    @Override
    protected long estimateRange(Integer start, Integer stop, boolean includeStart, boolean includeStop, boolean isMin) {
        return rangeSelectivity(start,stop,includeStart,includeStop,isMin);
    }

    @Override
    public long selectivity(int value) {
        IntColumnStatistics ics = (IntColumnStatistics)columnStats;
        //we are outside the range, so we know it's 0
        if(value<ics.min()) return 0l;
        else if(value==ics.min()) return ics.minCount(); //we are asking for the min, we have an exact number!
        else if(value>ics.max()) return 0l; //we are outside the range, so it's 0

        //if we have an exact count provided by Frequent elements, use that
        IntFrequencyEstimate est = ((IntFrequentElements)ics.topK()).countEqual(value);
        if(est.count()>0) return est.count();
        else {
            return uniformEstimate();
        }
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
        if(start==stop){
            if(!includeStart || !includeStop) return 0l; //empty interval has no data
            else return selectivity(start); //return equals
        }
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
        double baseEstimate = a*stop+b;
        baseEstimate-=a*start+b;
        includeStart = includeStart &&!isMin;

        //adjust using Frequent Elements
        IntFrequentElements ife = (IntFrequentElements)columnStats.topK();
        //if we are the min value, don't include the start key in frequent elements
        Set<IntFrequencyEstimate> intFrequencyEstimates = ife.frequentBetween(start, stop, includeStart, includeStop);
        return uniformRangeCount(includeStart,includeStop, baseEstimate,intFrequencyEstimates);
    }

}
