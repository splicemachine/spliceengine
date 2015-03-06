package com.splicemachine.stats.estimate;

import com.splicemachine.stats.frequency.BooleanFrequentElements;

/**
 * @author Scott Fines
 *         Date: 3/5/15
 */
public class BooleanDistribution implements Distribution<Boolean> {
    private final long nullCount;
    private final BooleanFrequentElements elements;

    public BooleanDistribution(long nullCount,BooleanFrequentElements elements) {
        this.nullCount = nullCount;
        this.elements = elements;
    }

    @Override
    public long selectivity(Boolean element) {
        if(element==null) return nullCount;
        if(element==Boolean.TRUE)
            return elements.equalsTrue().count();
        else return elements.equalsFalse().count();
    }

    @Override
    public long rangeSelectivity(Boolean start, Boolean stop, boolean includeStart, boolean includeStop) {
        if(start==null){
            if(stop==null) return elements.equalsTrue().count()+elements.equalsFalse().count();
            else return rangeSelectivity(Boolean.TRUE,stop,true,includeStop);
        }else if(stop==null){
            return rangeSelectivity(start,Boolean.FALSE,includeStart,true);
        }else
        if(start==stop){
            if(includeStart && includeStop) return elements.equal(start).count();
            return 0l;
        }else{
            //since start!=stop, we assume that start<stop, which means that start = TRUE, and stop = FALSE
            //so we just have to compute the edge components
            long count = 0;
            if(includeStart) count+=elements.equalsTrue().count();
            if(includeStop) count+=elements.equalsFalse().count();
            return count;
        }
    }
}
