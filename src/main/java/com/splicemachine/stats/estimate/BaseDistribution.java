package com.splicemachine.stats.estimate;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.stats.frequency.ObjectFrequentElements;

import java.util.Comparator;


/**
 * A Base Distribution class. This is mainly intended to handle null counts and range overlaps
 * appropriately.
 *
 * @author Scott Fines
 *         Date: 3/4/15
 */
public abstract class BaseDistribution<T> implements Distribution<T> {
    protected ColumnStatistics<T> columnStats;
    protected Comparator<? super T> comparator;

    /**
     * Create a new BaseDistribution, with the specified statistics and comparator.
     *
     * @param columnStats the base statistics to use
     * @param comparator the comparator to use. NOTE: This comparator must be <em>null-safe</em>--that is,
     *                   it must be able to compare a null element.
     */
    protected BaseDistribution(ColumnStatistics<T> columnStats,Comparator<? super T> comparator) {
        this.columnStats = columnStats;
        this.comparator = comparator;
    }

    @Override
    public long selectivity(T element) {
        if(element==null)
            return columnStats.nullCount();

        //adjust for outside the range of known data
        int compare = comparator.compare(columnStats.minValue(),element);
        if(compare>0) return 0l;
        else if(compare==0) return columnStats.minCount();
        compare = comparator.compare(columnStats.maxValue(),element);
        if(compare<0) return 0l;
        
        //if we have an exact count from the frequent elements, use it.
        @SuppressWarnings({ "rawtypes", "unchecked" })
		FrequencyEstimate est = ((ObjectFrequentElements) columnStats.topK()).equal(element);
		if (est != null && est.count()>0) return est.count();

        return estimateEquals(element);
    }

    @Override
    public long rangeSelectivity(T start, T stop, boolean includeStart, boolean includeStop) {
        T distributionMin = columnStats.minValue();
        T distributionMax = columnStats.maxValue();
        if(distributionMin==null||distributionMax==null){
            /*
             * The only way a distribution min or max can be null is if the entire partition is null.
             * In that case, we have to do special checks to determine whether or not null is included
             * in the range. In order for that that happen, both elements must be null
             */
            if(start==null && stop==null) return columnStats.nullCount();
            else return 0;
        }
        if(start==null){
            if(stop==null)
                return columnStats.nullCount()+columnStats.nonNullCount(); //asking for entire range
            else
                return rangeSelectivity(distributionMin, stop, true, includeStop); //asking for everything before stop
        }else if(stop==null){
            return rangeSelectivity(start, distributionMax,includeStart,true); //asking for everything after start
        }else{
            int valueCompare = comparator.compare(start,stop);
            assert valueCompare<=0: "Cannot compute selectivity: start occurs after stop!";
            if(valueCompare==0){
                if(!includeStart || !includeStop) return 0l; //empty set has no data
                else
                    return selectivity(start); //match the equals clause
            }
            if(start.equals(stop)&&(!includeStart ||!includeStop)) return 0l; //empty set has no data
            //adjust for situations in which we are outside the range
            int compare = comparator.compare(distributionMin,stop);
            if(compare>0||(!includeStop && compare==0)){
                //The start of data happens completely after the end of the requested range, so nothing there
                return 0l;
            }else if(includeStop && compare==0){
                //we include only the end point, so just estimate equal(min)
                return selectivity(stop);
            }
            //we know that we aren't before the start of the range, but what about after the end?
            compare = comparator.compare(distributionMax,start);
            if(compare<0||(!includeStart && compare==0)){
                //we are completely after the end of the range, so nothing to see
                return 0l;
            }else if(includeStart && compare==0){
                //we include only the start, so just estimate equals(max)
                return selectivity(start);
            }
            /*
             * We know that we definitely overlap the range of data, but perhaps we only overlap the
             * endpoints. If so, adjust the range to either [min,b) or [a,max)
             */
            compare = comparator.compare(distributionMin,start);
            if(compare>0)
                start = distributionMin;
            boolean isMin = compare==0;
            compare = comparator.compare(distributionMax,stop);
            if(compare<0)
                stop = distributionMax;
            if(isMin && compare==0 && includeStart && includeStop){
                //we are asking for min and max, so just return the known row count
                return columnStats.nonNullCount();
            }
            return estimateRange(start, stop, includeStart, includeStop, isMin);
        }
    }

    protected abstract long estimateRange(T start, T stop, boolean includeStart, boolean includeStop, boolean isMin);

    protected long getAdjustedRowCount() {
        long rowCount = columnStats.nonNullCount();
        FrequentElements<T> frequentElements = columnStats.topK();
        rowCount-=frequentElements.totalFrequentElements();
        if(frequentElements.equal(columnStats.minValue()).count()<=0)
            rowCount-=columnStats.minCount();
        return rowCount;
    }

    protected long getAdjustedCardinality(){
        long cardinality = columnStats.cardinality();
        FrequentElements<T> frequentElements = columnStats.topK();
        cardinality-=frequentElements.allFrequentElements().size();
        return cardinality;
    }

    protected abstract long estimateEquals(T element);

}
