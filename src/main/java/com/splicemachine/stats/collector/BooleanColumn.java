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
                nullCount,frequencies.equalsTrue().count());
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
