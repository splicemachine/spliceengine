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

import com.splicemachine.stats.FloatUpdateable;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
public class FloatMinMaxCollector implements MinMaxCollector<Float>,FloatUpdateable {
    private float currentMin;
    private long currentMinCount;
    private float currentMax;
    private long currentMaxCount;

    @Override
    public void update(float item, long count) {
        if(item==currentMin)
            currentMinCount+=count;
        else if(currentMin>item) {
            currentMin = item;
            currentMinCount = count;
        }
        if(item==currentMax)
            currentMaxCount+=count;
        else if(currentMax<item) {
            currentMax = item;
            currentMaxCount = count;
        }
    }

    @Override public void update(float item) { update(item,1l); }

    @Override
    public void update(Float item) {
        assert item!=null: "Cannot order null elements";
        update(item.floatValue());
    }

    @Override
    public void update(Float item, long count) {
        assert item!=null: "Cannot order null elements!";
        update(item.floatValue(),count);
    }

    @Override public Float minimum() { return currentMin; }
    @Override public Float maximum() { return currentMax; }
    @Override public long minCount() { return currentMinCount; }
    @Override public long maxCount() { return currentMaxCount; }

    public float max(){ return currentMax; }
    public float min(){ return currentMin; }

    public static FloatMinMaxCollector newInstance() {
        FloatMinMaxCollector collector = new FloatMinMaxCollector();
        collector.currentMin = Float.MAX_VALUE;
        collector.currentMax = Float.MIN_VALUE;
        return collector;
    }
}
