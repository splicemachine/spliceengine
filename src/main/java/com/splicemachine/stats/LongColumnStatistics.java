package com.splicemachine.stats;

import com.splicemachine.encoding.Encoder;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.cardinality.LongCardinalityEstimator;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.estimate.LongDistribution;
import com.splicemachine.stats.estimate.UniformLongDistribution;
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
public class LongColumnStatistics extends BaseColumnStatistics<Long> {
    private LongCardinalityEstimator cardinalityEstimator;
    private LongFrequentElements frequentElements;
    private long min;
    private long max;
    private LongDistribution distribution;

    public LongColumnStatistics(int columnId,LongCardinalityEstimator cardinalityEstimator,
                                LongFrequentElements frequentElements,
                                long min,
                                long max,
                                long totalBytes,
                                long totalCount,
                                long nullCount,
                                long minCount) {
        super(columnId, totalBytes, totalCount, nullCount,minCount);
        this.cardinalityEstimator = cardinalityEstimator;
        this.frequentElements = frequentElements;
        this.min = min;
        this.max = max;
        this.distribution = new UniformLongDistribution(this);
    }

    @Override public Distribution<Long> getDistribution() { return distribution; }
    @Override public long cardinality() { return cardinalityEstimator.getEstimate(); }
    @Override public FrequentElements<Long> topK() { return frequentElements; }
    @Override public Long minValue() { return min; }
    @Override public Long maxValue() { return max; }
    public long min() { return min; }
    public long max() { return max; }

    @Override
    public ColumnStatistics<Long> getClone() {
        return new LongColumnStatistics(columnId,cardinalityEstimator.newCopy(),
                frequentElements.newCopy(),
                min,
                max,
                totalBytes,
                totalCount,
                nullCount,
                minCount);
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
            BaseColumnStatistics.write(item, encoder);
            encoder.writeLong(item.min);
            encoder.writeLong(item.max);
            CardinalityEstimators.longEncoder().encode(item.cardinalityEstimator, encoder);
            FrequencyCounters.longEncoder().encode(item.frequentElements,encoder);
        }

        @Override
        public LongColumnStatistics decode(DataInput decoder) throws IOException {
            int columnId = decoder.readInt();
            long totalBytes = decoder.readLong();
            long totalCount = decoder.readLong();
            long nullCount = decoder.readLong();
            long minCount = decoder.readLong();
            long min = decoder.readLong();
            long max = decoder.readLong();
            LongCardinalityEstimator cardinalityEstimator = CardinalityEstimators.longEncoder().decode(decoder);
            LongFrequentElements frequentElements = FrequencyCounters.longEncoder().decode(decoder);
            return new LongColumnStatistics(columnId,cardinalityEstimator,frequentElements,min,max,totalBytes,totalCount,nullCount,minCount);
        }
    }

}
