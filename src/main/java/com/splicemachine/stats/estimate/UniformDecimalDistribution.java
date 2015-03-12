package com.splicemachine.stats.estimate;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.utils.ComparableComparator;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 3/5/15
 */
public class UniformDecimalDistribution extends BaseDistribution<BigDecimal> {
    public UniformDecimalDistribution(ColumnStatistics<BigDecimal> columnStats) {
        super(columnStats, ComparableComparator.<BigDecimal>newComparator());
    }

    @Override
    protected long estimateRange(BigDecimal start, BigDecimal stop, boolean includeStart, boolean includeStop, boolean isMin) {
        long actualCardinality = columnStats.cardinality();
        BigDecimal dist = stop.subtract(start);
        BigDecimal card = BigDecimal.valueOf(actualCardinality);
        BigDecimal maxDist = columnStats.maxValue().subtract(columnStats.minValue());
        BigDecimal adjustedCardinality = dist.divide(maxDist,MathContext.DECIMAL64).multiply(card);

        /*
         * By definition, dist < maxDist, so dist/maxDist < 1. This means that adjustedCardinality < cardinality,
         * which is a long. Therefore, we can take a long here to determine how many elements to use
         */
        long adjCard = adjustedCardinality.longValue();
        long rowsPerEntry = getAdjustedRowCount()/actualCardinality;

        long baseEstimate = rowsPerEntry*adjCard;

        if(!includeStart)
            baseEstimate-=rowsPerEntry;
        else if(isMin){
            baseEstimate-=rowsPerEntry;
            baseEstimate+=rowsPerEntry;
        }
        if(includeStop)
            baseEstimate+=rowsPerEntry;

        //if we are the min value, don't include the start key in frequent elements
        includeStart = includeStart &&!isMin;
        //now adjust for the frequent elements in the set
        Set<? extends FrequencyEstimate<BigDecimal>> fe = columnStats.topK().frequentElementsBetween(start,stop,includeStart,includeStop);
        baseEstimate-= fe.size()*rowsPerEntry;
        for(FrequencyEstimate<BigDecimal> est:fe){
            baseEstimate+=est.count();
        }
        return baseEstimate;
    }
}
