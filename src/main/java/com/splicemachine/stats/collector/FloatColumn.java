package com.splicemachine.stats.collector;

import com.splicemachine.stats.FloatColumnStatistics;
import com.splicemachine.stats.cardinality.FloatCardinalityEstimator;
import com.splicemachine.stats.frequency.FloatFrequencyCounter;
import com.splicemachine.stats.order.FloatMinMaxCollector;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
class FloatColumn implements FloatColumnStatsCollector {
    private final FloatCardinalityEstimator cardinalityEstimator;
    private final FloatFrequencyCounter frequencyCounter;
    private final FloatMinMaxCollector minMaxCollector;

    private long nullCount;
    private long totalBytes;
    private long count;

    /*The number of frequent elements to keep*/
    private int topK;

    public FloatColumn(FloatCardinalityEstimator cardinalityEstimator,
                      FloatFrequencyCounter frequencyCounter,
                      FloatMinMaxCollector minMaxCollector,
                      int topK) {
        this.cardinalityEstimator = cardinalityEstimator;
        this.frequencyCounter = frequencyCounter;
        this.minMaxCollector = minMaxCollector;
        this.topK = topK;
    }

    @Override
    public FloatColumnStatistics build() {
        return new FloatColumnStatistics(cardinalityEstimator,
                frequencyCounter.frequentElements(topK),
                minMaxCollector.min(),
                minMaxCollector.max(),
                totalBytes,
                count,
                nullCount );
    }

    @Override public void updateSize(int size) { totalBytes+=size; }
    @Override public void updateNull() { updateNull(1l); }
    @Override public void update(float item) { update(item,1l); }
    @Override public void update(Float item) { update(item,count); }

    @Override
    public void updateNull(long count) {
        nullCount+=count;
        this.count+=count;
    }

    @Override
    public void update(float item, long count) {
        cardinalityEstimator.update(item,count);
        frequencyCounter.update(item,count);
        minMaxCollector.update(item,count);
        this.count+=count;
    }

    @Override
    public void update(Float item, long count) {
        if(item==null)
            updateNull(count);
        else update(item.floatValue(),count);
    }
}
