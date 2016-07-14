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

package com.splicemachine.stats.collector;

import com.splicemachine.stats.IntColumnStatistics;
import com.splicemachine.stats.cardinality.IntCardinalityEstimator;
import com.splicemachine.stats.frequency.IntFrequencyCounter;
import com.splicemachine.stats.order.IntMinMaxCollector;

/**
 * A Statistics collector for an integer column.
 *
 * @author Scott Fines
 *         Date: 2/23/15
 */
class IntColumn implements IntColumnStatsCollector {
    private final int columnId;
    private final IntCardinalityEstimator cardinalityEstimator;
    private final IntFrequencyCounter frequencyCounter;
    private final IntMinMaxCollector minMaxCollector;

    private long nullCount;
    private long totalBytes;
    private long count;

    /*The number of frequent elements to keep*/
    private int topK;

    public IntColumn(int columnId,
                     IntCardinalityEstimator cardinalityEstimator,
                     IntFrequencyCounter frequencyCounter,
                     IntMinMaxCollector minMaxCollector,
                     int topK) {
        this.cardinalityEstimator = cardinalityEstimator;
        this.frequencyCounter = frequencyCounter;
        this.minMaxCollector = minMaxCollector;
        this.topK = topK;
        this.columnId = columnId;
    }

    @Override
    public IntColumnStatistics build() {
        return new IntColumnStatistics(columnId,
                cardinalityEstimator,
                frequencyCounter.frequentElements(topK),
                minMaxCollector.min(),
                minMaxCollector.max(),
                totalBytes,
                count,
                nullCount,
                minMaxCollector.minCount());
    }

    @Override public void updateSize(int size) { totalBytes+=size; }
    @Override public void updateNull() { updateNull(1l); }
    @Override public void update(int item) {  update(item,1l);  }
    @Override public void update(Integer item) { update(item,1l); }

    @Override
    public void updateNull(long count) {
        nullCount+=count;
        this.count+=count;
    }

    @Override
    public void update(int item, long count) {
        cardinalityEstimator.update(item,count);
        frequencyCounter.update(item,count);
        minMaxCollector.update(item,count);
        this.count+=count;
    }

    @Override
    public void update(Integer item, long count) {
        if(item==null)
            updateNull(count);
        else update(item.intValue(),count);
    }
}
