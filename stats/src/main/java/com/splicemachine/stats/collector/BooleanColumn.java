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

import com.splicemachine.stats.BooleanColumnStatistics;
import com.splicemachine.stats.frequency.BooleanFrequencyCounter;
import com.splicemachine.stats.frequency.BooleanFrequentElements;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
class BooleanColumn implements BooleanColumnStatsCollector {
    private final int columnId;
    private final BooleanFrequencyCounter frequencyCounter;

    private long nullCount;
    private long totalBytes;
    private long count;

    public BooleanColumn(int columnId,BooleanFrequencyCounter frequencyCounter) {
        this.columnId = columnId;
        this.frequencyCounter = frequencyCounter;
    }

    @Override
    public BooleanColumnStatistics build() {
        BooleanFrequentElements frequencies = frequencyCounter.frequencies();
        return new BooleanColumnStatistics(
                columnId,
                frequencies,
                totalBytes,
                count,
                nullCount);
    }

    @Override public void updateSize(int size) { totalBytes+=size; }
    @Override public void updateNull() { updateNull(1l); }
    @Override public void update(boolean item) { update(item,1l); }
    @Override public void update(Boolean item) { update(item,1l); }

    @Override
    public void updateNull(long count) {
        nullCount+=count;
        this.count+=count;
    }

    @Override
    public void update(boolean item, long count) {
        frequencyCounter.update(item,count);
        this.count+=count;
    }

    @Override
    public void update(Boolean item, long count) {
        if(item==null)
            updateNull(count);
        else update(item.booleanValue(),count);
    }
}
