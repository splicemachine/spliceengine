package com.splicemachine.stats;

import com.splicemachine.encoding.Encoder;
import com.splicemachine.primitives.ByteComparator;
import com.splicemachine.stats.cardinality.BytesCardinalityEstimator;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.frequency.BytesFrequentElements;
import com.splicemachine.stats.frequency.FrequencyCounters;
import com.splicemachine.stats.frequency.FrequentElements;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
public class BytesColumnStatistics implements ColumnStatistics<byte[]> {
    private BytesCardinalityEstimator cardinalityEstimator;
    private BytesFrequentElements frequentElements;
    private byte[] min;
    private byte[] max;

    private long totalBytes;
    private long totalCount;
    private long nullCount;
    private ByteComparator byteComparator;

    public BytesColumnStatistics(BytesCardinalityEstimator cardinalityEstimator,
                                  BytesFrequentElements frequentElements,
                                  ByteComparator byteComparator,
                                  byte[] min,
                                  byte[] max,
                                  long totalBytes,
                                  long totalCount,
                                  long nullCount) {
        this.cardinalityEstimator = cardinalityEstimator;
        this.byteComparator = byteComparator;
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
    @Override public FrequentElements<byte[]> topK() { return frequentElements; }
    @Override public byte[] minValue() { return min; }
    @Override public byte[] maxValue() { return max; }
    public byte[] min(){ return min; }
    public byte[] max(){ return max; }
    @Override public long avgColumnWidth() { return totalBytes/totalCount;}

    @Override
    public ColumnStatistics<byte[]> getClone() {
        return new BytesColumnStatistics(cardinalityEstimator.getClone(),
                frequentElements.getClone(),
                byteComparator,
                min,
                max,
                totalBytes,
                totalCount,
                nullCount);
    }

    @Override
    public ColumnStatistics<byte[]> merge(ColumnStatistics<byte[]> other) {
        assert other instanceof BytesColumnStatistics: "Cannot merge statistics of type "+ other.getClass();
        BytesColumnStatistics o = (BytesColumnStatistics)other;
        cardinalityEstimator = cardinalityEstimator.merge(o.cardinalityEstimator);
        frequentElements = frequentElements.merge(o.frequentElements);
        if(byteComparator.compare(o.min,min)>0)
            min = o.min;
        if(byteComparator.compare(o.max, max)>0)
            max = o.max;
        totalBytes+=o.totalBytes;
        totalCount+=o.totalCount;
        nullCount+=o.nullCount;
        return this;
    }

    public Encoder<BytesColumnStatistics> encoder(){
        return new EncDec(byteComparator);
    }

    static class EncDec implements Encoder<BytesColumnStatistics> {
        private final ByteComparator byteComparator;

        public EncDec(ByteComparator byteComparator) {
            this.byteComparator = byteComparator;
        }

        @Override
        public void encode(BytesColumnStatistics item,DataOutput encoder) throws IOException {
            encoder.writeInt(item.min.length);
            encoder.write(item.min);
            encoder.writeInt(item.max.length);
            encoder.write(item.max);
            encoder.writeLong(item.totalBytes);
            encoder.writeLong(item.totalCount);
            encoder.writeLong(item.nullCount);
            CardinalityEstimators.bytesEncoder().encode(item.cardinalityEstimator, encoder);
            FrequencyCounters.byteArrayEncoder().encode(item.frequentElements, encoder);
        }

        @Override
        public BytesColumnStatistics decode(DataInput decoder) throws IOException {
            byte[] min = new byte[decoder.readInt()];
            decoder.readFully(min);
            byte[] max = new byte[decoder.readInt()];
            decoder.readFully(max);
            long totalBytes = decoder.readLong();
            long totalCount = decoder.readLong();
            long nullCount = decoder.readLong();
            BytesCardinalityEstimator cardinalityEstimator = CardinalityEstimators.bytesEncoder().decode(decoder);
            BytesFrequentElements frequentElements = FrequencyCounters.byteArrayEncoder().decode(decoder);
            return new BytesColumnStatistics(cardinalityEstimator, frequentElements,
                    byteComparator,min,max,totalBytes,totalCount,nullCount);
        }
    }
}
