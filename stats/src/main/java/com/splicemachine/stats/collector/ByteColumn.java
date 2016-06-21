package com.splicemachine.stats.collector;

import com.splicemachine.stats.ByteColumnStatistics;
import com.splicemachine.stats.cardinality.ByteCardinalityEstimator;
import com.splicemachine.stats.frequency.ByteFrequencyCounter;
import com.splicemachine.stats.order.ByteMinMaxCollector;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
class ByteColumn implements ByteColumnStatsCollector {

    private final int columnId;
    private final ByteCardinalityEstimator cardinalityEstimator;
    private final ByteFrequencyCounter frequencyCounter;
    private final ByteMinMaxCollector minMaxCollector;

    private long nullCount;
    private long totalBytes;
    private long count;

    /*The number of frequent elements to keep*/
    private int topK;

    public ByteColumn(int columnId,
                      ByteCardinalityEstimator cardinalityEstimator,
                      ByteFrequencyCounter frequencyCounter,
                      ByteMinMaxCollector minMaxCollector,
                      int topK) {
        this.columnId = columnId;
        this.cardinalityEstimator = cardinalityEstimator;
        this.frequencyCounter = frequencyCounter;
        this.minMaxCollector = minMaxCollector;
        this.topK = topK;
    }

    @Override
    public ByteColumnStatistics build() {
        return new ByteColumnStatistics(
                columnId,
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
    @Override public void update(byte item) { update(item,1l); }
    @Override public void update(Byte item) { update(item,count); }

    @Override
    public void updateNull(long count) {
        nullCount+=count;
        this.count+=count;
    }

    @Override
    public void update(byte item, long count) {
        cardinalityEstimator.update(item,count);
        frequencyCounter.update(item,count);
        minMaxCollector.update(item,count);
        this.count+=count;
    }

    @Override
    public void update(Byte item, long count) {
        if(item==null)
            updateNull(count);
        else update(item.byteValue(),count);
    }
}
