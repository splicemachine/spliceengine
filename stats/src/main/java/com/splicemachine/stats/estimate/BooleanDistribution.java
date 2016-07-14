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

    public long cardinality(Boolean start,Boolean stop,boolean includeStart,boolean includeStop){
        if(start==null){
            start=true;
            includeStart = true;
        }

        if(stop==null){
            stop = false;
            includeStop = true;
        }
        boolean hasTrue = elements.equalsTrue().count()>0;
        boolean hasFalse = elements.equalsFalse().count()>0;

        if(start){
            if(stop){
                //start and stop
                if(includeStart && includeStop) return hasTrue? 1:0l;
                else return 0l;
            }else{
                //start and !stop
                long c = 0l;
                if(includeStart && hasTrue)
                    c++;
                if(includeStop && hasFalse)
                    c++;
                return c;
            }
        }else if(stop){
            //!start and stop
            return 0l;
        }else{
            //!start and !stop
            if(includeStart &&includeStop)
                return hasFalse? 1:0l;
            return 0l;
        }
    }

    @Override
    public Boolean minValue(){
        return elements.equalsTrue().count()>0? Boolean.TRUE: elements.equalsFalse().count()>0? Boolean.FALSE: null;
    }

    @Override public long minCount(){ return elements.equalsTrue().count(); }

    @Override
    public Boolean maxValue(){
        return elements.equalsFalse().count()>0? Boolean.FALSE: elements.equalsTrue().count()>0? Boolean.TRUE: null;
    }

    @Override public long totalCount(){ return elements.totalFrequentElements(); }

    public long cardinality(){
        if(elements.equalsTrue().count()>0){
            if(elements.equalsFalse().count()>0) return 2;
            else return 1;
        }else if(elements.equalsFalse().count()>0) return 1;
        else return 0;
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
