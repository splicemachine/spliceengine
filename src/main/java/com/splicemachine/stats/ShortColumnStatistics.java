package com.splicemachine.stats;

import com.splicemachine.encoding.Encoder;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.cardinality.ShortCardinalityEstimator;
import com.splicemachine.stats.frequency.FrequencyCounters;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.stats.frequency.ShortFrequentElements;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public class ShortColumnStatistics implements ColumnStatistics<Short> {

    private ShortCardinalityEstimator cardinalityEstimator;
    private ShortFrequentElements frequentElements;
    private short min;
    private short max;

    private long totalBytes;
    private long totalCount;
    private long nullCount;

    public ShortColumnStatistics(ShortCardinalityEstimator cardinalityEstimator,
                                  ShortFrequentElements frequentElements,
                                  short min,
                                  short max,
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
    @Override public FrequentElements<Short> topK() { return frequentElements; }
    @Override public Short minValue() { return min; }
    @Override public Short maxValue() { return max; }
    public short min(){ return min; }
    public short max(){ return max; }
    @Override public long avgColumnWidth() { return totalBytes/totalCount;}

    @Override
    public ColumnStatistics<Short> getClone() {
        return new ShortColumnStatistics(cardinalityEstimator.newCopy(),
                frequentElements.newCopy(),
                min,
                max,
                totalBytes,
                totalCount,
                nullCount);
    }

    @Override
    public ColumnStatistics<Short> merge(ColumnStatistics<Short> other) {
        assert other instanceof ShortColumnStatistics: "Cannot merge statistics of type "+ other.getClass();
        ShortColumnStatistics o = (ShortColumnStatistics)other;
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

    public static Encoder<ShortColumnStatistics> encoder(){
        return EncDec.INSTANCE;
    }

    static class EncDec implements Encoder<ShortColumnStatistics> {
        public static final EncDec INSTANCE = new EncDec();

        @Override
        public void encode(ShortColumnStatistics item,DataOutput encoder) throws IOException {
            encoder.writeShort(item.min);
            encoder.writeShort(item.max);
            encoder.writeLong(item.totalBytes);
            encoder.writeLong(item.totalCount);
            encoder.writeLong(item.nullCount);
            CardinalityEstimators.shortEncoder().encode(item.cardinalityEstimator, encoder);
            FrequencyCounters.shortEncoder().encode(item.frequentElements,encoder);
        }

        @Override
        public ShortColumnStatistics decode(DataInput decoder) throws IOException {
            short min = decoder.readShort();
            short max = decoder.readShort();
            long totalBytes = decoder.readLong();
            long totalCount = decoder.readLong();
            long nullCount = decoder.readLong();
            ShortCardinalityEstimator cardinalityEstimator = CardinalityEstimators.shortEncoder().decode(decoder);
            ShortFrequentElements frequentElements = FrequencyCounters.shortEncoder().decode(decoder);
            return new ShortColumnStatistics(cardinalityEstimator,frequentElements,min,max,totalBytes,totalCount,nullCount);
        }
    }
}
