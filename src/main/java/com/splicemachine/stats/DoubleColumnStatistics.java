package com.splicemachine.stats;

import com.splicemachine.encoding.Encoder;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.cardinality.DoubleCardinalityEstimator;
import com.splicemachine.stats.frequency.DoubleFrequentElements;
import com.splicemachine.stats.frequency.FrequencyCounters;
import com.splicemachine.stats.frequency.FrequentElements;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public class DoubleColumnStatistics implements ColumnStatistics<Double> {
    private DoubleCardinalityEstimator cardinalityEstimator;
    private DoubleFrequentElements frequentElements;
    private double min;
    private double max;

    private long totalBytes;
    private long totalCount;
    private long nullCount;

    public DoubleColumnStatistics(DoubleCardinalityEstimator cardinalityEstimator,
                               DoubleFrequentElements frequentElements,
                               double min,
                               double max,
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
    @Override public FrequentElements<Double> topK() { return frequentElements; }
    @Override public Double minValue() { return min; }
    @Override public Double maxValue() { return max; }
    public double min(){ return min; }
    public double max(){ return max; }
    @Override public long avgColumnWidth() { return totalBytes/totalCount;}

    @Override
    public ColumnStatistics<Double> merge(ColumnStatistics<Double> other) {
        assert other instanceof DoubleColumnStatistics: "Cannot merge statistics of type "+ other.getClass();
        DoubleColumnStatistics o = (DoubleColumnStatistics)other;
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

    static class EncDec implements Encoder<DoubleColumnStatistics> {
        public static final EncDec INSTANCE = new EncDec();

        @Override
        public void encode(DoubleColumnStatistics item,DataOutput encoder) throws IOException {
            encoder.writeDouble(item.min);
            encoder.writeDouble(item.max);
            encoder.writeLong(item.totalBytes);
            encoder.writeLong(item.totalCount);
            encoder.writeLong(item.nullCount);
            CardinalityEstimators.doubleEncoder().encode(item.cardinalityEstimator, encoder);
            FrequencyCounters.doubleEncoder().encode(item.frequentElements,encoder);
        }

        @Override
        public DoubleColumnStatistics decode(DataInput decoder) throws IOException {
            double min = decoder.readDouble();
            double max = decoder.readDouble();
            long totalBytes = decoder.readLong();
            long totalCount = decoder.readLong();
            long nullCount = decoder.readLong();
            DoubleCardinalityEstimator cardinalityEstimator = CardinalityEstimators.doubleEncoder().decode(decoder);
            DoubleFrequentElements frequentElements = FrequencyCounters.doubleEncoder().decode(decoder);
            return new DoubleColumnStatistics(cardinalityEstimator,frequentElements,min,max,totalBytes,totalCount,nullCount);
        }
    }
}

