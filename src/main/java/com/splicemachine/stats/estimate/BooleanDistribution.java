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

    public long selectivity(boolean element){
        return element? elements.equalsTrue().count(): elements.equalsFalse().count();
    }

    @Override
    public long selectivity(Boolean element) {
        if(element==null) return nullCount;
        if(element==Boolean.TRUE)
            return elements.equalsTrue().count();
        else return elements.equalsFalse().count();
    }

    public long rangeSelectivity(boolean start, boolean stop, boolean includeStart, boolean includeStop){
        if(start==stop){
            if(includeStart && includeStop) return elements.equals(start).count();
            else return 0l; //empty set has 0 elements
        }else{
            /*
             * We now have the following states
             *
             * start && !stop
             * !start && stop
             *
             * And the second option is invalid, because TRUE comes before FALSE in our arbitrary ordering,
             * so we only have start && !stop
             */
            assert start : "Cannot get range selectivity, stop comes before start!";
            long count = 0l;
            if(includeStart) count+=elements.equalsTrue().count();
            if(includeStop) count+=elements.equalsFalse().count();
            return count;
        }
    }

    @Override
    public long rangeSelectivity(Boolean start, Boolean stop, boolean includeStart, boolean includeStop) {
        if(start==null){
            if(stop==null) return elements.equalsTrue().count()+elements.equalsFalse().count();
            else return rangeSelectivity(true,stop.booleanValue(),true,includeStop);
        }else if(stop==null){
            return rangeSelectivity(start.booleanValue(),false,includeStart,true);
        }else
            return rangeSelectivity(start.booleanValue(),stop.booleanValue(),includeStart,includeStop);
    }
}
