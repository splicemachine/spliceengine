package com.splicemachine.stats;


import com.splicemachine.encoding.Encoder;
import com.splicemachine.stats.cardinality.CardinalityEstimator;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.frequency.FrequencyCounters;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.stats.frequency.ObjectFrequentElements;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
public class ComparableColumnStatistics<T extends Comparable<T>> implements ColumnStatistics<T> {
    private CardinalityEstimator<T> cardinalityEstimator;
    private FrequentElements<T> frequentElements;
    private T min;
    private T max;

    private long totalBytes;
    private long totalCount;
    private long nullCount;

    public ComparableColumnStatistics(CardinalityEstimator<T> cardinalityEstimator,
                                      FrequentElements<T> frequentElements,
                                      T min,
                                      T max,
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
    @Override public FrequentElements<T> topK() { return frequentElements; }
    @Override public T minValue() { return min; }
    @Override public T maxValue() { return max; }
    @Override public long avgColumnWidth() { return totalBytes/totalCount;}

    @Override
    public ColumnStatistics<T> getClone() {
        return new ComparableColumnStatistics<>(cardinalityEstimator.getClone(),
                frequentElements.getClone(),
                //TODO -sf- is this safe?
                min,
                max,
                totalBytes,totalCount,nullCount);
    }

    @Override
    public ColumnStatistics<T> merge(ColumnStatistics<T> other) {
        assert other instanceof ComparableColumnStatistics : "Cannot merge statistics of type "+ other.getClass();
        ComparableColumnStatistics<T> o = (ComparableColumnStatistics<T>)other;
        cardinalityEstimator = cardinalityEstimator.merge(o.cardinalityEstimator);
        frequentElements = frequentElements.merge(o.frequentElements);
        if(o.min.compareTo(min)>0)
            min = o.min;
        if(o.max.compareTo(max)<0)
            max = o.max;
        totalBytes+=o.totalBytes;
        totalCount+=o.totalCount;
        nullCount+=o.nullCount;
        return this;
    }

    public static <T extends Comparable<T>> Encoder<ComparableColumnStatistics<T>> encoder(Encoder<T> typeEncoder){
        return new EncDec<>(typeEncoder);
    }

    static class EncDec<T extends Comparable<T>> implements Encoder<ComparableColumnStatistics<T>> {
        private Encoder<T> valueEncoder;

        public EncDec(Encoder<T> valueEncoder) {
            this.valueEncoder = valueEncoder;
        }

        @Override
        public void encode(ComparableColumnStatistics<T> item,DataOutput encoder) throws IOException {
            valueEncoder.encode(item.min,encoder);
            valueEncoder.encode(item.max,encoder);
            encoder.writeLong(item.totalBytes);
            encoder.writeLong(item.totalCount);
            encoder.writeLong(item.nullCount);
            CardinalityEstimators.<T>objectEncoder().encode(item.cardinalityEstimator, encoder);
            FrequencyCounters.objectEncoder(valueEncoder).encode(item.frequentElements,encoder);
        }

        @Override
        public ComparableColumnStatistics<T> decode(DataInput decoder) throws IOException {
            T min = valueEncoder.decode(decoder);
            T max = valueEncoder.decode(decoder);
            long totalBytes = decoder.readLong();
            long totalCount = decoder.readLong();
            long nullCount = decoder.readLong();
            CardinalityEstimator<T> cardinalityEstimator = CardinalityEstimators.<T>objectEncoder().decode(decoder);
            FrequentElements<T> frequentElements = FrequencyCounters.objectEncoder(valueEncoder).decode(decoder);
            return new ComparableColumnStatistics<>(cardinalityEstimator,frequentElements,min,max,totalBytes,totalCount,nullCount);
        }
    }
}
