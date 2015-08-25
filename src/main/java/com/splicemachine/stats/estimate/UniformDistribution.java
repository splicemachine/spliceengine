package com.splicemachine.stats.estimate;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.frequency.FrequencyEstimate;

import java.util.Comparator;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 7/23/15
 */
public abstract class UniformDistribution<T> extends BaseDistribution<T>{

    public UniformDistribution(ColumnStatistics<T> columnStats,Comparator<? super T> comparator){
        super(columnStats,comparator);
    }

    protected final long uniformEstimate(){

        /*
         * We are using a 'Compressed Histogram' strategy, where we store exact counts for
         * the most frequent elements, and the *remaining* elements are stored as a uniform
         * distribution. That means that we don't include the frequent elements in our cardinality
         * or row count estimates
         */
        long adjustedRowCount = getAdjustedRowCount();
        long cardinality = getAdjustedCardinality();
        //if there is no cardinality, there can't be a row count either.
        if(cardinality<=0) return 0l;
        if (cardinality > adjustedRowCount && adjustedRowCount > 0) {
            cardinality = adjustedRowCount;
        }
        return adjustedRowCount/cardinality;
    }

    @Override
    protected long estimateEquals(T element){
        return uniformEstimate();
    }

    protected final long uniformRangeCount(boolean includeStart,boolean includeStop,
                                           double baseEstimate, Set<? extends FrequencyEstimate<T>> frequentElements){
        long perRowCount = uniformEstimate();
        if(perRowCount==0){
            /*
             * If perRowCount==0, then we have a situation where either
             *
             * 1) cardinality <=0
             * 2) adjustedRowCount <=0
             *
             * in either case, this is telling us that all of the elements in the table are contained
             * in the FrequentElements for this distribution, which implies that the uniform portion of
             * the compressed histogram is in fact 0 (there is no uniform histogram to compute). In this case,
             * we remove the baseEstimate, because all it's giving us is garbage anyway.
             */
            baseEstimate=0;
        }
        if(!includeStart)
            baseEstimate-=perRowCount;
        if(includeStop)
            baseEstimate+=perRowCount;
        baseEstimate-= perRowCount*frequentElements.size();
        for(FrequencyEstimate<T> est: frequentElements){
            baseEstimate+=est.count()-est.error();
        }
        return (long)baseEstimate;
    }
}
