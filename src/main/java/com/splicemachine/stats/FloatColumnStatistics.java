package com.splicemachine.stats;

import com.splicemachine.encoding.Encoder;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.cardinality.FloatCardinalityEstimator;
import com.splicemachine.stats.frequency.FloatFrequentElements;
import com.splicemachine.stats.frequency.FrequencyCounters;
import com.splicemachine.stats.frequency.FrequentElements;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public class FloatColumnStatistics implements ColumnStatistics<Float> {
    private FloatCardinalityEstimator cardinalityEstimator;
    private FloatFrequentElements frequentElements;
    private float min;
    private float max;

    private long totalBytes;
    private long totalCount;
    private long nullCount;

    public FloatColumnStatistics(FloatCardinalityEstimator cardinalityEstimator,
                                 FloatFrequentElements frequentElements,
                                 float min,
                                 float max,
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
    @Override public FrequentElements<Float> topK() { return frequentElements; }
    @Override public Float minValue() { return min; }
    @Override public Float maxValue() { return max; }
    public float min(){ return min; }
    public float max(){ return max; }
    @Override public long avgColumnWidth() { return totalBytes/totalCount;}

    @Override
    public ColumnStatistics<Float> merge(ColumnStatistics<Float> other) {
        assert other instanceof FloatColumnStatistics: "Cannot merge statistics of type "+ other.getClass();
        FloatColumnStatistics o = (FloatColumnStatistics)other;
        cardinalityEstimator = cardinalityEstimator.merge(o.cardinalityEstimator);
        frequentElements = frequentElements.merge(o.frequentElements);
        if(o.min<min)
            min = o.min;
        if(o.max>max)
            max = o.max;
        totalBytes+=o.totalBytes;
        totalCount+=o.totalCount;
        nullCount+=o.nullCount;
        return this;
    }

    static class EncDec implements Encoder<FloatColumnStatistics> {
        public static final EncDec INSTANCE = new EncDec();

        @Override
        public void encode(FloatColumnStatistics item,DataOutput encoder) throws IOException {
            encoder.writeFloat(item.min);
            encoder.writeFloat(item.max);
            encoder.writeLong(item.totalBytes);
            encoder.writeLong(item.totalCount);
            encoder.writeLong(item.nullCount);
            CardinalityEstimators.floatEncoder().encode(item.cardinalityEstimator, encoder);
            FrequencyCounters.floatEncoder().encode(item.frequentElements,encoder);
        }

        @Override
        public FloatColumnStatistics decode(DataInput decoder) throws IOException {
            float min = decoder.readFloat();
            float max = decoder.readFloat();
            long totalBytes = decoder.readLong();
            long totalCount = decoder.readLong();
            long nullCount = decoder.readLong();
            FloatCardinalityEstimator cardinalityEstimator = CardinalityEstimators.floatEncoder().decode(decoder);
            FloatFrequentElements frequentElements = FrequencyCounters.floatEncoder().decode(decoder);
            return new FloatColumnStatistics(cardinalityEstimator,frequentElements,min,max,totalBytes,totalCount,nullCount);
        }
    }
}
