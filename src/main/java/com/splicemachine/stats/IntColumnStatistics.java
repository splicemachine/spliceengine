package com.splicemachine.stats;

import com.splicemachine.encoding.Encoder;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.cardinality.IntCardinalityEstimator;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.estimate.UniformIntDistribution;
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
public class IntColumnStatistics extends BaseColumnStatistics<Integer>{
    private IntCardinalityEstimator cardinalityEstimator;
    private IntFrequentElements frequentElements;
    private int min;
    private int max;

    private transient Distribution<Integer> distribution;


    public IntColumnStatistics(int columnId,
                               IntCardinalityEstimator cardinalityEstimator,
                               IntFrequentElements frequentElements,
                               int min,
                               int max,
                               long totalBytes,
                               long totalCount,
                               long nullCount,
                               long minCount) {
        super(columnId, totalBytes, totalCount, nullCount,minCount);
        this.cardinalityEstimator = cardinalityEstimator;
        this.frequentElements = frequentElements;
        this.min = min;
        this.max = max;
        this.distribution = new UniformIntDistribution(this);
    }


    @Override
    public Distribution<Integer> getDistribution() {
        return distribution;
    }

    @Override public long cardinality() { return cardinalityEstimator.getEstimate(); }
    @Override public FrequentElements<Integer> topK() { return frequentElements; }
    @Override public Integer minValue() { return min; }
    @Override public Integer maxValue() { return max; }
    public int min(){ return min; }
    public int max(){ return max; }

    @Override
    public ColumnStatistics<Integer> getClone() {
        return new IntColumnStatistics(columnId,cardinalityEstimator.newCopy(),
                frequentElements.newCopy(),
                min,
                max,
                totalBytes,
                totalCount,
                nullCount,minCount);
    }

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

    public static Encoder<IntColumnStatistics> encoder(){
        return EncDec.INSTANCE;
    }

    static class EncDec implements Encoder<IntColumnStatistics> {
        public static final EncDec INSTANCE = new EncDec();

        @Override
        public void encode(IntColumnStatistics item,DataOutput encoder) throws IOException {
            BaseColumnStatistics.write(item, encoder);
            encoder.writeInt(item.min);
            encoder.writeInt(item.max);
            CardinalityEstimators.intEncoder().encode(item.cardinalityEstimator, encoder);
            FrequencyCounters.intEncoder().encode(item.frequentElements,encoder);
        }

        @Override
        public IntColumnStatistics decode(DataInput decoder) throws IOException {
            int columnId = decoder.readInt();
            long totalBytes = decoder.readLong();
            long totalCount = decoder.readLong();
            long nullCount = decoder.readLong();
            long minCount = decoder.readLong();
            int min = decoder.readInt();
            int max = decoder.readInt();
            IntCardinalityEstimator cardinalityEstimator = CardinalityEstimators.intEncoder().decode(decoder);
            IntFrequentElements frequentElements = FrequencyCounters.intEncoder().decode(decoder);
            return new IntColumnStatistics(columnId,cardinalityEstimator,frequentElements,min,max,totalBytes,totalCount,nullCount,minCount);
        }
    }
}
