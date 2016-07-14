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

import java.util.Collection;

/**
 * @author Scott Fines
 *         Date: 8/12/15
 */
public class CompositeDistribution<T extends Comparable<T>> implements Distribution<T>{
    private final Collection<Distribution<T>> subDistributions;

    public CompositeDistribution(Collection<Distribution<T>> subDistributions){
        this.subDistributions=subDistributions;
    }

    @Override
    public T minValue(){
        T min = null;
        for(Distribution<T> dist:subDistributions){
            T m = dist.minValue();
            if(min==null||min.compareTo(m)>0)
                min = m;
        }
        return min;
    }

    @Override
    public long minCount(){
        T min = minValue();
        long c = 0l;
        for(Distribution<T> d:subDistributions){
            c+=d.selectivity(min);
        }
        return c;
    }

    @Override
    public T maxValue(){
        T max = null;
        for(Distribution<T> dist:subDistributions){
            T m = dist.maxValue();
            if(max==null||max.compareTo(m)<0)
                max = m;
        }
        return max;
    }

    @Override
    public long totalCount(){
        long tc = 0l;
        for(Distribution<T> dist: subDistributions){
            tc+=dist.totalCount();
        }
        return tc;
    }

    @Override
    public long selectivity(T element){
        long s = 0l;
        for(Distribution<T> dist:subDistributions){
            s+=dist.selectivity(element);
        }
        return s;
    }

    @Override
    public long rangeSelectivity(T start,T stop,boolean includeStart,boolean includeStop){
        long s = 0l;
        for(Distribution<T> dist:subDistributions){
            s+=dist.rangeSelectivity(start,stop,includeStart,includeStop);
        }
        return s;
    }
}
