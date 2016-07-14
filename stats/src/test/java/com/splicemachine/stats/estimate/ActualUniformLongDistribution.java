/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
    @Override public Long minValue(){ return min(); }
    @Override public long max(){ return max; }
    @Override public Long maxValue(){ return max(); }

    public long cardinalityBefore(long stop,boolean includeStop){
        if(stop>max) {
            stop=max;
            includeStop = true;
        }
        if(stop<min||(stop==min &&!includeStop)) return 0l;
        long card = stop-min;
        if(includeStop)card++;

        return card;
    }

    public long cardinalityAfter(long start,boolean includeStart){
        if(start<min){
            start = min;
            includeStart = true;
        }
        if(start>max||(start==max &&!includeStart)) return 0l;
        long card = max-start+1;
        if(!includeStart) card--;
        return card;
    }

    public long rangeCardinality(long start,long stop,boolean includeStart,boolean includeStop){
        if(start==stop){
            if(includeStart&&includeStop) return 1l;
            return 0l;
        }
        if(start<min){
            start = min;
            includeStart = true;
        }
        if(stop>max){
            stop = max;
            includeStop = true;
        }

        long card = stop-start;
        if(includeStop) card++;
        if(!includeStart)card--;
        return card;
    }


    @Override public long minCount(){ return recordsPerEntry; }


    public long cardinality(){
        return (min-max)+1;
    }

    @Override
    public long totalCount(){
        return recordsPerEntry*(max-min+1);
    }

    public long cardinality(Long start,Long stop,boolean includeStart,boolean includeStop){
        if(start==null){
            if(stop==null) return cardinality();
            return cardinalityBefore(stop,includeStop);
        }else if(stop==null) return cardinalityAfter(start,includeStart);
        else{
            return rangeCardinality(start,stop,includeStart,includeStop);
        }
    }

    @Override
    public long selectivity(Long element){
        assert element!=null: "Cannot estimate selectivity of null elements!";
        return selectivity(element.longValue());
    }

    public long rangeSelectivity(Long start,Long stop,boolean includeStart,boolean includeStop){
        assert start!=null: "Cannot estimate the selectivity of null elements";
        assert stop!=null: "Cannot estimate the selectivity of null elements";
        return rangeSelectivity(start.longValue(),stop.longValue(),includeStart,includeStop);
    }
}
