package com.splicemachine.stats;

import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.cardinality.IntCardinalityEstimator;
import com.splicemachine.stats.frequency.FrequencyCounters;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.stats.frequency.IntFrequentElements;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public class IntColumnStatistics implements ColumnStatistics<Integer>{
    private IntCardinalityEstimator cardinalityEstimator;
    private IntFrequentElements frequentElements;
    private int min;
    private int max;

    private long totalBytes;
    private long totalCount;
    private long nullCount;

    public IntColumnStatistics(IntCardinalityEstimator cardinalityEstimator,
                               IntFrequentElements frequentElements,
                               int min,
                               int max,
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
    @Override public FrequentElements<Integer> topK() { return frequentElements; }
    @Override public Integer minValue() { return min; }
    @Override public Integer maxValue() { return max; }
    public int min(){ return min; }
    public int max(){ return max; }
    @Override public long avgColumnWidth() { return totalBytes/totalCount;}

    @Override
    public ColumnStatistics<Integer> merge(ColumnStatistics<Integer> other) {
        assert other instanceof IntColumnStatistics: "Cannot merge statistics of type "+ other.getClass();
        IntColumnStatistics o = (IntColumnStatistics)other;
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

    @Override
    public void encode(DataOutput encoder) throws IOException {
        encoder.writeInt(min);
        encoder.writeInt(max);
        encoder.writeLong(totalBytes);
        encoder.writeLong(totalCount);
        encoder.writeLong(nullCount);
        CardinalityEstimators.intEncoder().encode(cardinalityEstimator, encoder);
        FrequencyCounters.intEncoder().encode(frequentElements,encoder);
    }

    @Override
    public void decode(DataInput decoder) throws IOException {
        min  = decoder.readInt();
        max = decoder.readInt();
        totalBytes = decoder.readLong();
        totalCount = decoder.readLong();
        nullCount = decoder.readLong();
        cardinalityEstimator = CardinalityEstimators.intEncoder().decode(decoder);
        frequentElements = FrequencyCounters.intEncoder().decode(decoder);
    }
}
