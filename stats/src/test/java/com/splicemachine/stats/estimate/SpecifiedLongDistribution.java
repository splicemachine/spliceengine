package com.splicemachine.stats.estimate;

/**
 * @author Scott Fines
 *         Date: 6/30/15
 */
public class SpecifiedLongDistribution implements LongDistribution{
    private final long[] counts;
    private final long min;
    private final long max;

    public SpecifiedLongDistribution(long[] counts,long min,long max){
        this.counts=counts;
        this.min=min;
        this.max=max;
    }

    public long cardinalityBefore(long stop,boolean includeStop){
        if(stop<min||(stop==min &&!includeStop)) return 0l;
        int s = (int)(stop-min);
        if(includeStop)s++;
        if(s>counts.length) s=counts.length;
        int p = 0;
        while(p<s){
            p++;
        }
        return p;
    }

    public long cardinalityAfter(long start,boolean includeStart){
        if(start>max||(start==max &&!includeStart)) return 0l;
        int p = (int)(start-min);
        if(!includeStart)p++;
        while(p<counts.length){
            p++;
        }
        return p;
    }

    public long rangeCardinality(long start,long stop,boolean includeStart,boolean includeStop){
        if(stop<min) return 0l;
        else if(stop==min){
            if(includeStop) return 1l;
            else return 0l;
        }else if(stop>max||(stop==max && includeStop)) return cardinalityAfter(start,includeStart);

        if(start>max) return 0l;
        else if(start==max){
            if(includeStart) return 1l;
            else return 0l;
        }else if(start<min||(start==min && includeStart)) return cardinalityBefore(stop,includeStop);

        int p = (int)(start-min);
        if(!includeStart)p++;
        if(p<0)
            p = 0;
        int s = (int)(stop-min);
        if(includeStop)
            s++;
        if(s>counts.length)
            s = counts.length;
        while(p<s){
            p++;
        }
        return p;
    }

    public long cardinality(Long start,Long stop,boolean includeStart,boolean includeStop){
        if(start==null){
            if(stop==null){
                return counts.length;
            }else return cardinalityBefore(stop,includeStop);
        }else if(stop==null) return cardinalityAfter(start,includeStart);
        else return rangeCardinality(start,stop,includeStart,includeStop);
    }

    @Override public Long minValue(){ return min(); }
    @Override public Long maxValue(){ return max(); }
    @Override public long minCount(){ return counts[0]; }


    public long cardinality(){ return counts.length; }

    @Override
    public long selectivity(long value){
        if(value<min) return 0l;
        return counts[(int)(value-min)];
    }

    @Override
    public long selectivityBefore(long stop,boolean includeStop){
        if(stop<min||(stop==min &&!includeStop)) return 0l;
        int p = 0;
        int s = (int)(stop-min);
        if(includeStop)s++;
        if(s>counts.length) s=counts.length;
        long count = 0l;
        while(p<s){
            count+=counts[p];
            p++;
        }
        return count;
    }

    @Override
    public long selectivityAfter(long start,boolean includeStart){
        if(start>max||(start==max &&!includeStart)) return 0l;
        int p = (int)(start-min);
        if(!includeStart)p++;
        long count=0;
        while(p<counts.length){
            count+=counts[p];
            p++;
        }
        return count;
    }

    @Override
    public long rangeSelectivity(long start,long stop,boolean includeStart,boolean includeStop){
        if(stop<min) return 0l;
        else if(stop==min){
            if(includeStop) return counts[0];
            else return 0l;
        }else if(stop>max||(stop==max && includeStop)) return selectivityAfter(start,includeStart);

        if(start>max) return 0l;
        else if(start==max){
            if(includeStart) return counts[(int)(max-min)];
            else return 0l;
        }else if(start<min||(start==min && includeStart)) return selectivityBefore(stop,includeStop);

        int p = (int)(start-min);
        if(!includeStart)p++;
        if(p<0)
            p = 0;
        int s = (int)(stop-min);
        if(includeStop)
            s++;
        if(s>counts.length)
            s = counts.length;
        long count = 0;
        while(p<s){
            count+=counts[p];
            p++;
        }
        return count;
    }

    @Override public long min(){ return min; }
    @Override public long max(){ return max; }

    @Override
    public long totalCount(){
        long c = 0;
        //noinspection ForLoopReplaceableByForEach
        for(int i=0;i<counts.length;i++){
            c+=counts[i];
        }
        return c;
    }

    @Override
    public long selectivity(Long element){
        assert element!=null: "Cannot estimate selectivity of null elements!";
        return selectivity(element.longValue());
    }

    @Override
    public long rangeSelectivity(Long start,Long stop,boolean includeStart,boolean includeStop){
        assert start!=null: "Cannot estimate selectivity of null elements!";
        assert stop!=null: "Cannot estimate selectivity of null elements!";
        return rangeSelectivity(start.longValue(),stop.longValue(),includeStart,includeStop);
    }
}
