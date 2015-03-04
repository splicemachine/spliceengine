package com.splicemachine.stats;

import com.splicemachine.encoding.Encoder;
import com.splicemachine.stats.cardinality.ByteCardinalityEstimator;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.frequency.ByteFrequentElements;
import com.splicemachine.stats.frequency.FrequencyCounters;
import com.splicemachine.stats.frequency.FrequentElements;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public class ByteColumnStatistics implements ColumnStatistics<Byte>{
    private ByteCardinalityEstimator cardinalityEstimator;
    private ByteFrequentElements frequentElements;
    private byte min;
    private byte max;

    private long totalBytes;
    private long totalCount;
    private long nullCount;

    public ByteColumnStatistics(ByteCardinalityEstimator cardinalityEstimator,
                                  ByteFrequentElements frequentElements,
                                  byte min,
                                  byte max,
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
    @Override public FrequentElements<Byte> topK() { return frequentElements; }
    @Override public Byte minValue() { return min; }
    @Override public Byte maxValue() { return max; }
    public byte min(){ return min; }
    public byte max(){ return max; }
    @Override public long avgColumnWidth() { return totalBytes/totalCount;}

    @Override
    public ColumnStatistics<Byte> getClone() {
        return new ByteColumnStatistics(cardinalityEstimator.getClone(),
                frequentElements.getClone(),
                min,max,totalBytes,totalCount,nullCount);
    }

    @Override
    public ColumnStatistics<Byte> merge(ColumnStatistics<Byte> other) {
        assert other instanceof ByteColumnStatistics: "Cannot merge statistics of type "+ other.getClass();
        ByteColumnStatistics o = (ByteColumnStatistics)other;
        cardinalityEstimator = cardinalityEstimator.merge(o.cardinalityEstimator);
        frequentElements = (ByteFrequentElements)frequentElements.merge(o.frequentElements);
        if(o.min<min)
            min = o.min;
        if(o.max>max)
            max = o.max;
        totalBytes+=o.totalBytes;
        totalCount+=o.totalCount;
        nullCount+=o.nullCount;
        return this;
    }
    public static Encoder<ByteColumnStatistics> encoder(){
        return EncDec.INSTANCE;
    }

    static class EncDec implements Encoder<ByteColumnStatistics> {
        public static final EncDec INSTANCE = new EncDec();

        @Override
        public void encode(ByteColumnStatistics item,DataOutput encoder) throws IOException {
            encoder.writeByte(item.min);
            encoder.writeByte(item.max);
            encoder.writeLong(item.totalBytes);
            encoder.writeLong(item.totalCount);
            encoder.writeLong(item.nullCount);
            CardinalityEstimators.byteEncoder().encode(item.cardinalityEstimator, encoder);
            FrequencyCounters.byteEncoder().encode(item.frequentElements,encoder);
        }

        @Override
        public ByteColumnStatistics decode(DataInput decoder) throws IOException {
            byte min = decoder.readByte();
            byte max = decoder.readByte();
            long totalBytes = decoder.readLong();
            long totalCount = decoder.readLong();
            long nullCount = decoder.readLong();
            ByteCardinalityEstimator cardinalityEstimator = CardinalityEstimators.byteEncoder().decode(decoder);
            ByteFrequentElements frequentElements = FrequencyCounters.byteEncoder().decode(decoder);
            return new ByteColumnStatistics(cardinalityEstimator,frequentElements,min,max,totalBytes,totalCount,nullCount);
        }
    }
}
