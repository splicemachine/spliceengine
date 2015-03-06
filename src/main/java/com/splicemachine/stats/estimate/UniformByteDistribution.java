package com.splicemachine.stats.estimate;

import com.splicemachine.stats.ByteColumnStatistics;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.frequency.ByteFrequencyEstimate;
import com.splicemachine.stats.frequency.ByteFrequentElements;
import com.splicemachine.utils.ComparableComparator;

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 3/5/15
 */
public class UniformByteDistribution extends BaseDistribution<Byte> implements ByteDistribution{
    public UniformByteDistribution(ColumnStatistics<Byte> columnStats) {
        super(columnStats, ComparableComparator.<Byte>newComparator());
    }


    @Override
    public long selectivity(byte value) {
        ByteColumnStatistics bcs = (ByteColumnStatistics)columnStats;
        if(value==bcs.min())
            return bcs.minCount();

        ByteFrequentElements bfe = (ByteFrequentElements)bcs.topK();
        ByteFrequencyEstimate byteFrequencyEstimate = bfe.countEqual(value);
        if(byteFrequencyEstimate.count()>0) return byteFrequencyEstimate.count();

        /*
         * We don't have a frequent element, so assume that we have a uniform distribution of
         * other elements. This estimate is computed by the formula
         *
         * R = (N-Nmin)/cardinality
         *
         * where N = the total row count - the size of the min record (if it isn't a frequent element) - size of
         * all frequent elements
         */
        long adjustedRowCount = getAdjustedRowCount();
        return adjustedRowCount/columnStats.cardinality();
    }

    @Override
    public long selectivityBefore(byte stop, boolean includeStop) {
        ByteColumnStatistics bcs = (ByteColumnStatistics)columnStats;
        byte min = bcs.min();
        if(stop<min ||(!includeStop && stop==min)) return 0l;

        return rangeSelectivity(min,stop,true,includeStop);
    }

    @Override
    public long selectivityAfter(byte start, boolean includeStart) {
        ByteColumnStatistics bcs = (ByteColumnStatistics)columnStats;
        byte max = bcs.max();
        if(start>max||(!includeStart && start==max)) return 0l;

        return rangeSelectivity(start,max,includeStart,true);
    }

    @Override
    protected long estimateEquals(Byte element) {
        return selectivity(element.byteValue());
    }

    @Override
    public long rangeSelectivity(byte start, byte stop, boolean includeStart, boolean includeStop) {
        ByteColumnStatistics bcs = (ByteColumnStatistics)columnStats;
        byte min = bcs.min();
        byte max = bcs.max();

        if(stop< min ||(!includeStop && stop== min)) return 0l;
        else if(includeStop && stop== min)
            return selectivity(stop);
        if(start> max ||(!includeStart && start== max)) return 0l;
        else if(includeStart && start == max)
            return selectivity(start);

        /*
         * Now adjust the range to deal with running off the end points of the range
         */
        boolean isMin = false;
        if(start<=min) {
            start = min;
            isMin = true;
        }
        if(stop> max)
            stop = max;

        return rangeSelectivity(start,stop,includeStart,includeStop,isMin);
    }

    @Override
    protected long estimateRange(Byte start, Byte stop, boolean includeStart, boolean includeStop, boolean isMin) {
        return rangeSelectivity(start, stop, includeStart, includeStop,isMin);
    }

    @Override
    protected long getAdjustedRowCount() {
        long rowCount = columnStats.nonNullCount();
        ByteFrequentElements frequentElements = (ByteFrequentElements)columnStats.topK();
        rowCount-=frequentElements.totalFrequentElements();
        if(frequentElements.equal(((ByteColumnStatistics)columnStats).min()).count()<=0)
            rowCount-=columnStats.minCount();
        return rowCount;
    }

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private long rangeSelectivity(byte start, byte stop, boolean includeStart, boolean includeStop, boolean isMin) {
        /*
         * Compute the base estimate, then adjust it using Frequent elements.
         *
         * The base estimate is computed using the formula
         *
         * baseEst = (adjustedRowCount*(stop-start))/cardinality
         *
         */
        long adjustedRowCount = getAdjustedRowCount();
        long perRowCount = adjustedRowCount/columnStats.cardinality();
        long baseEst = perRowCount*(stop-start);
        if(!includeStart) {
            baseEst -= perRowCount;
        }else if(isMin){
            //adjust the minimum
            baseEst-=perRowCount;
            baseEst+=columnStats.minCount();
        }

        if(includeStop) baseEst+=perRowCount;

        /*
         * Now adjust using Frequent Elements
         */
        ByteFrequentElements bfe = (ByteFrequentElements)columnStats.topK();
        Set<ByteFrequencyEstimate> frequencyEstimates = bfe.frequentBetween(start, stop, includeStart, includeStop);
        baseEst-=frequencyEstimates.size()*perRowCount;
        for(ByteFrequencyEstimate est:frequencyEstimates){
            baseEst+=est.count();
        }
        return baseEst;
    }

}
