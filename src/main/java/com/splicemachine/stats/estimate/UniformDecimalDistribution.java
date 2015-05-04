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
    private final BigDecimal a;
    private final BigDecimal b;

    public UniformDecimalDistribution(ColumnStatistics<BigDecimal> columnStats) {
        super(columnStats, ComparableComparator.<BigDecimal>newComparator());

        BigDecimal at = BigDecimal.valueOf(columnStats.nonNullCount()-columnStats.minCount());
        at = at.divide(columnStats.maxValue().subtract(columnStats.minValue()),MathContext.DECIMAL64);

        this.a = at;
        this.b = BigDecimal.valueOf(columnStats.nonNullCount()).subtract(a.multiply(columnStats.maxValue()));
    }

    @Override
    protected long estimateRange(BigDecimal start, BigDecimal stop, boolean includeStart, boolean includeStop, boolean isMin) {
        BigDecimal baseE = a.multiply(stop).add(b).subtract(a.multiply(start).add(b));
        long rowsPerEntry = getPerRowCount();
        /*
         * This is safe, because the linear function we used has a max of maxValue on the range [minValue,maxValue),
         * with a maximum value of rowCount (we built the linear function to do this). Since rowCount is a long,
         * the value of baseE *MUST* fit within a long (and therefore, within a double).
         */
        double baseEstimate = baseE.doubleValue();
        if(!includeStart){
            baseEstimate-=rowsPerEntry;
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
        return (long)baseEstimate;
    }

    private long getPerRowCount() {
        return getAdjustedRowCount()/columnStats.cardinality();
    }
}
