package com.splicemachine.stats;

import com.splicemachine.encoding.Encoder;
import com.splicemachine.stats.cardinality.ByteCardinalityEstimator;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.estimate.ByteDistribution;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.estimate.UniformByteDistribution;
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
public class ByteColumnStatistics extends BaseColumnStatistics<Byte> {
    private ByteCardinalityEstimator cardinalityEstimator;
    private ByteFrequentElements frequentElements;
    private byte min;
    private byte max;

    private transient ByteDistribution distribution;

    public ByteColumnStatistics(int columnId,
                                ByteCardinalityEstimator cardinalityEstimator,
                                ByteFrequentElements frequentElements,
                                byte min,
                                byte max,
                                long totalBytes,
                                long totalCount,
                                long nullCount,
                                long minCount) {
        super(columnId, totalBytes, totalCount, nullCount,minCount);
        this.cardinalityEstimator = cardinalityEstimator;
        this.frequentElements = frequentElements;
        this.min = min;
        this.max = max;
        this.distribution = new UniformByteDistribution(this);
    }

    @Override public long cardinality() { return cardinalityEstimator.getEstimate(); }
    @Override public FrequentElements<Byte> topK() { return frequentElements; }
    @Override public Byte minValue() { return min; }
    @Override public Byte maxValue() { return max; }
    public byte min(){ return min; }
    public byte max(){ return max; }

    @Override
    public ColumnStatistics<Byte> getClone() {
        return new ByteColumnStatistics(columnId,cardinalityEstimator.getClone(),
                frequentElements.getClone(),
                min,max,totalBytes,totalCount,nullCount,minCount);
    }

    @Override public Distribution<Byte> getDistribution() { return distribution; }

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
            BaseColumnStatistics.write(item, encoder);
            encoder.writeByte(item.min);
            encoder.writeByte(item.max);
            CardinalityEstimators.byteEncoder().encode(item.cardinalityEstimator, encoder);
            FrequencyCounters.byteEncoder().encode(item.frequentElements,encoder);
        }

        @Override
        public ByteColumnStatistics decode(DataInput decoder) throws IOException {
            int columnId = decoder.readInt();
            long totalBytes = decoder.readLong();
            long totalCount = decoder.readLong();
            long nullCount = decoder.readLong();
            long minCount = decoder.readLong();
            byte min = decoder.readByte();
            byte max = decoder.readByte();
            ByteCardinalityEstimator cardinalityEstimator = CardinalityEstimators.byteEncoder().decode(decoder);
            ByteFrequentElements frequentElements = FrequencyCounters.byteEncoder().decode(decoder);
            return new ByteColumnStatistics(columnId,cardinalityEstimator,frequentElements,min,max,totalBytes,totalCount,nullCount,minCount);
        }
    }
}
