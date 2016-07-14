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

package com.splicemachine.stats.order;

import com.splicemachine.stats.Updateable;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
public class ComparableMinMaxCollector<T extends Comparable<T>> implements MinMaxCollector<T>,Updateable<T> {
    private T currentMin;
    private long minCount;
    private T currentMax;
    private long maxCount;

    @Override public T minimum() { return currentMin; }
    @Override public T maximum() { return currentMax; }

    @Override public long minCount() { return minCount; }
    @Override public long maxCount() { return maxCount; }

    @Override public void update(T item) { update(item,1l); }

    @Override
    public void update(T item, long count) {
        if(currentMin==null){
            currentMin = item;
            minCount = count;
        }else{
            int compare = currentMin.compareTo(item);
            if(compare==0)
                minCount+=count;
            else if(compare>0) {
                currentMin = item;
                minCount = count;
            }
        }

        if(currentMax==null){
            currentMax = item;
            maxCount = count;
        }else{
            int compare = currentMax.compareTo(item);
            if(compare==0)
                maxCount+=count;
            else if(compare<0) {
                currentMax = item;
                maxCount = count;
            }
        }
    }
}
