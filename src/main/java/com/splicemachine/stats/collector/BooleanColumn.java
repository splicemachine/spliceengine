package com.splicemachine.stats.collector;

import com.splicemachine.stats.BooleanColumnStatistics;
import com.splicemachine.stats.frequency.BooleanFrequencyCounter;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
public class BooleanColumn implements BooleanColumnStatsCollector {
    private final BooleanFrequencyCounter frequencyCounter;

    private long nullCount;
    private long totalBytes;
    private long count;

    public BooleanColumn(BooleanFrequencyCounter frequencyCounter) {
        this.frequencyCounter = frequencyCounter;
    }

    @Override
    public BooleanColumnStatistics build() {
        return new BooleanColumnStatistics(
                frequencyCounter.frequencies(),
                totalBytes,
                count,
                nullCount );
    }

    @Override public void updateSize(int size) { totalBytes+=size; }
    @Override public void updateNull() { updateNull(1l); }
    @Override public void update(boolean item) { update(item,1l); }
    @Override public void update(Boolean item) { update(item,count); }

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
