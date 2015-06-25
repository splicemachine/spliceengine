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

        if(columnStats.nonNullCount()==0){
            /*
             * We have an empty distribution. Make sure that our linear interpolation
             * is 0
             */
            this.a = BigDecimal.ZERO;
            this.b = BigDecimal.ZERO;
        }else if(columnStats.maxValue().compareTo(columnStats.minValue())==0){
            /*
             * There is only one record in the distribution, so treat the line as a constant
             * function
             */
            this.a = BigDecimal.ZERO;
            this.b = BigDecimal.valueOf(columnStats.minCount());
        }else{
            /*
             * Create a linear function to interpolate between the min and max values.
             */
            BigDecimal at=BigDecimal.valueOf(columnStats.nonNullCount()-columnStats.minCount());
            at=at.divide(columnStats.maxValue().subtract(columnStats.minValue()),MathContext.DECIMAL64);

            this.a=at;
            this.b=BigDecimal.valueOf(columnStats.nonNullCount()).subtract(a.multiply(columnStats.maxValue()));
        }
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
            baseEstimate+=est.count()-est.error();
        }
        return (long)baseEstimate;
    }

    private long getPerRowCount() {
        return getAdjustedRowCount()/columnStats.cardinality();
    }
}
