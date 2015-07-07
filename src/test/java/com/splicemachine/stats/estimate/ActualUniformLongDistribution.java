package com.splicemachine.stats.estimate;

/**
 * @author Scott Fines
 *         Date: 6/30/15
 */
public class ActualUniformLongDistribution implements LongDistribution{
    private final long min;
    private final long max;
    private final long recordsPerEntry;

    public ActualUniformLongDistribution(long min,long max,long recordsPerEntry){
        this.min=min;
        this.max=max;
        this.recordsPerEntry=recordsPerEntry;
    }

    @Override
    public long selectivity(long value){
        if(value<min) return 0l;
        else if(value>=max) return 0l;
        else return recordsPerEntry;
    }

    @Override
    public long selectivityBefore(long stop,boolean includeStop){
        if(stop<min||(stop==min&&!includeStop)) return 0l;
        long selectivity = (stop-min)*recordsPerEntry;
        if(includeStop) selectivity+=recordsPerEntry;
        return selectivity;
    }

    @Override
    public long selectivityAfter(long start,boolean includeStart){
        if(start>max||(start==max && !includeStart)) return 0l;
        long selectivity = (max-start)*recordsPerEntry;
        if(!includeStart) selectivity-=recordsPerEntry;
        return selectivity;
    }

    @Override
    public long rangeSelectivity(long start,long stop,boolean includeStart,boolean includeStop){
        if(start==stop){
            if(includeStart && includeStop) return selectivity(start);
            else return 0l;
        }
        if(start>max||(start==max &&!includeStart)) return 0l;
        if(stop<min||(stop==min && !includeStop)) return 0l;
        long selectivity = (stop-start)*recordsPerEntry;
        if(includeStop) selectivity+=recordsPerEntry;
        if(!includeStart) selectivity-=recordsPerEntry;
        return selectivity;
    }

    @Override public long min(){ return min; }

    @Override public long max(){ return max; }

    @Override
    public long totalCount(){
        return recordsPerEntry*(max-min+1);
    }

    @Override
    public long selectivity(Long element){
        assert element!=null: "Cannot estimate selectivity of null elements!";
        return selectivity(element.longValue());
    }

    @Override
    public long rangeSelectivity(Long start,Long stop,boolean includeStart,boolean includeStop){
        assert start!=null: "Cannot estimate the selectivity of null elements";
        assert stop!=null: "Cannot estimate the selectivity of null elements";
        return rangeSelectivity(start.longValue(),stop.longValue(),includeStart,includeStop);
    }
}
