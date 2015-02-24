package com.splicemachine.stats.collector;

import com.splicemachine.primitives.ByteComparator;
import com.splicemachine.stats.BytesColumnStatistics;
import com.splicemachine.stats.cardinality.BytesCardinalityEstimator;
import com.splicemachine.stats.frequency.BytesFrequencyCounter;
import com.splicemachine.stats.order.BytesMinMaxCollector;

import java.nio.ByteBuffer;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
class BytesColumn implements BytesColumnStatsCollector {

    private final BytesCardinalityEstimator cardinalityEstimator;
    private final BytesFrequencyCounter frequencyCounter;
    private final BytesMinMaxCollector minMaxCollector;
    private final ByteComparator byteComparator;

    private long nullCount;
    private long totalBytes;
    private long count;

    /*The number of frequent elements to keep*/
    private int topK;

    public BytesColumn(BytesCardinalityEstimator cardinalityEstimator,
                       BytesFrequencyCounter frequencyCounter,
                       BytesMinMaxCollector minMaxCollector,
                       ByteComparator byteComparator, int topK) {
        this.cardinalityEstimator = cardinalityEstimator;
        this.frequencyCounter = frequencyCounter;
        this.minMaxCollector = minMaxCollector;
        this.byteComparator = byteComparator;
        this.topK = topK;
    }

    @Override
    public BytesColumnStatistics build() {
        return new BytesColumnStatistics(cardinalityEstimator,
                frequencyCounter.frequentElements(topK),
                byteComparator,
                minMaxCollector.minimum(),
                minMaxCollector.maximum(),
                totalBytes,
                count,
                nullCount );
    }

    @Override public void updateSize(int size) { totalBytes+=size; }
    @Override public void updateNull() { updateNull(1l); }

    @Override public void update(byte[] item) { update(item,0,item.length); }
    @Override public void update(byte[] item, long count) { update(item,0,item.length,count); }
    @Override public void update(byte[] bytes, int offset, int length) { update(bytes,offset,length,1l);  }
    @Override public void update(ByteBuffer bytes) { update(bytes,1l); }

    @Override
    public void update(byte[] bytes, int offset, int length, long count) {
        cardinalityEstimator.update(bytes,offset,length,count);
        frequencyCounter.update(bytes,offset,length,count);
        minMaxCollector.update(bytes,offset,length,count);
        this.count+=count;
        this.totalBytes+=length;
    }

    @Override
    public void update(ByteBuffer bytes, long count) {
        this.totalBytes+=bytes.remaining();
        cardinalityEstimator.update(bytes,count);
        frequencyCounter.update(bytes,count);
        minMaxCollector.update(bytes,count);
        this.count+=count;
    }

    @Override
    public void updateNull(long count) {
        nullCount+=count;
        this.count+=count;
    }

}
