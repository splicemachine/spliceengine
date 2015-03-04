package com.splicemachine.stats;

import com.splicemachine.encoding.Encoder;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.cardinality.LongCardinalityEstimator;
import com.splicemachine.stats.frequency.FrequencyCounters;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.stats.frequency.LongFrequentElements;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public class LongColumnStatistics implements ColumnStatistics<Long> {
    private LongCardinalityEstimator cardinalityEstimator;
    private LongFrequentElements frequentElements;
    private long min;
    private long max;
    private long totalBytes;
    private long totalCount;
    private long nullCount;

    public LongColumnStatistics(LongCardinalityEstimator cardinalityEstimator,
                                LongFrequentElements frequentElements,
                                long min,
                                long max,
                                long totalBytes,
                                long totalCount,
                                long nullCount) {
        this.cardinalityEstimator = cardinalityEstimator;
        this.frequentElements = frequentElements;
        this.min = min;
        this.max = max;
        this.totalBytes = totalBytes;
        this.totalCount = totalCount;
        this.nullCount = nullCount;
    }

    @Override public long cardinality() { return cardinalityEstimator.getEstimate(); }
    @Override public float nullFraction() { return ((float)nullCount)/totalCount; }
    @Override public long nullCount() { return nullCount; }
    @Override public FrequentElements<Long> topK() { return frequentElements; }
    @Override public Long minValue() { return min; }
    @Override public Long maxValue() { return max; }
    public long min() { return min; }
    public long max() { return max; }
    @Override public long avgColumnWidth() { return totalBytes/totalCount; }

    @Override
    public ColumnStatistics<Long> getClone() {
        return new LongColumnStatistics(cardinalityEstimator.newCopy(),
                frequentElements.newCopy(),
                min,
                max,
                totalBytes,
                totalCount,
                nullCount);
    }

    @Override
    public ColumnStatistics<Long> merge(ColumnStatistics<Long> other) {
        assert other instanceof LongColumnStatistics: "Cannot merge instance of type "+ other.getClass();
        LongColumnStatistics lo = (LongColumnStatistics)other;
        cardinalityEstimator = cardinalityEstimator.merge(lo.cardinalityEstimator);
        frequentElements = frequentElements.merge(lo.frequentElements);
        if(min>lo.min)
            min = lo.min;
        if(max<lo.max)
            max = lo.max;
        totalBytes+=lo.totalBytes;
        totalCount+=lo.totalCount;
        nullCount+=lo.nullCount;
        return this;
    }

    public static Encoder<LongColumnStatistics> encoder(){
        return EncDec.INSTANCE;
    }

    static class EncDec implements Encoder<LongColumnStatistics> {
        public static final EncDec INSTANCE = new EncDec();

        @Override
        public void encode(LongColumnStatistics item,DataOutput encoder) throws IOException {
            encoder.writeLong(item.min);
            encoder.writeLong(item.max);
            encoder.writeLong(item.totalBytes);
            encoder.writeLong(item.totalCount);
            encoder.writeLong(item.nullCount);
            CardinalityEstimators.longEncoder().encode(item.cardinalityEstimator, encoder);
            FrequencyCounters.longEncoder().encode(item.frequentElements,encoder);
        }

        @Override
        public LongColumnStatistics decode(DataInput decoder) throws IOException {
            long min = decoder.readLong();
            long max = decoder.readLong();
            long totalBytes = decoder.readLong();
            long totalCount = decoder.readLong();
            long nullCount = decoder.readLong();
            LongCardinalityEstimator cardinalityEstimator = CardinalityEstimators.longEncoder().decode(decoder);
            LongFrequentElements frequentElements = FrequencyCounters.longEncoder().decode(decoder);
            return new LongColumnStatistics(cardinalityEstimator,frequentElements,min,max,totalBytes,totalCount,nullCount);
        }
    }

}
