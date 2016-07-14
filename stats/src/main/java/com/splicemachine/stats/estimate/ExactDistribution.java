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

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * @author Scott Fines
 *         Date: 8/11/15
 */
public class ExactDistribution<T extends Comparable<T>> implements Distribution<T>{
    private final NavigableMap<T,Long> dataMap;

    public ExactDistribution(T...elements){
        dataMap=newMap();
        for(T element:elements){
            dataMap.put(element,dataMap.get(element)+1);
        }
    }

    public ExactDistribution(Iterable<T> elements){
        this.dataMap = newMap();
        for(T element:elements){
            dataMap.put(element,dataMap.get(element)+1);
        }
    }

    public long cardinality(T start,T stop,boolean includeStart,boolean includeStop){
        return dataMap.subMap(start,includeStart,stop,includeStop).size();
    }

    @Override
    public T minValue(){
        if(dataMap.size()==0) return null;
        return dataMap.firstEntry().getKey();
    }

    @Override
    public long minCount(){
        if(dataMap.size()==0) return 0l;
        return dataMap.firstEntry().getValue();
    }

    @Override
    public T maxValue(){
        if(dataMap.size()==0) return null;
        return dataMap.lastEntry().getKey();
    }

    @Override
    public long totalCount(){
        return rangeSelectivity(dataMap.firstKey(),dataMap.lastKey(),true,true);
    }

    public long cardinality(){
        return dataMap.size();
    }

    @Override
    public long selectivity(T element){
        return dataMap.get(element);
    }

    @Override
    public long rangeSelectivity(T start,T stop,boolean includeStart,boolean includeStop){
        NavigableMap<T, Long> subMap=dataMap.subMap(start,includeStart,stop,includeStop);
        long c = 0l;
        for(Map.Entry<T,Long> se:subMap.entrySet()){
            c+=se.getValue();
        }

        return c;
    }

    private NavigableMap<T, Long> newMap(){
        return new TreeMap<T,Long>(){
            @Override
            public Long get(Object key){
                Long count = super.get(key);
                if(count==null)
                    return 0l;
                else return count;
            }
        };
    }
}
