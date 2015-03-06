package com.splicemachine.stats;

import com.splicemachine.encoding.Encoder;
import com.splicemachine.stats.cardinality.CardinalityEstimator;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.estimate.DistributionFactory;
import com.splicemachine.stats.frequency.FrequencyCounters;
import com.splicemachine.stats.frequency.FrequentElements;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
public class ComparableColumnStatistics<T extends Comparable<T>> extends BaseColumnStatistics<T> {
    private CardinalityEstimator<T> cardinalityEstimator;
    private FrequentElements<T> frequentElements;
    private T min;
    private T max;
    private DistributionFactory<T> distributionFactory;


    public ComparableColumnStatistics(int columnId,
                                      CardinalityEstimator<T> cardinalityEstimator,
                                      FrequentElements<T> frequentElements,
                                      T min,
                                      T max,
                                      long totalBytes,
                                      long totalCount,
                                      long nullCount,
                                      long minCount,
                                      DistributionFactory<T> distributionFactory) {
        super(columnId, totalBytes, totalCount, nullCount,minCount);
        this.cardinalityEstimator = cardinalityEstimator;
        this.frequentElements = frequentElements;
        this.min = min;
        this.max = max;
        this.distributionFactory = distributionFactory;
    }

    @Override public long cardinality() { return cardinalityEstimator.getEstimate(); }
    @Override public FrequentElements<T> topK() { return frequentElements; }
    @Override public T minValue() { return min; }
    @Override public T maxValue() { return max; }

    @Override
    public Distribution<T> getDistribution() {
        return distributionFactory.newDistribution(this);
    }

    @Override
    public ColumnStatistics<T> getClone() {
        return new ComparableColumnStatistics<>(columnId,cardinalityEstimator.getClone(),
                frequentElements.getClone(),
                //TODO -sf- is this safe?
                min,
                max,
                totalBytes,totalCount,nullCount,minCount,distributionFactory);
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

    public static <T extends Comparable<T>> Encoder<ComparableColumnStatistics<T>> encoder(Encoder<T> typeEncoder,
                                                                                           DistributionFactory<T> distFactory){
        return new EncDec<>(typeEncoder,distFactory);
    }

    static class EncDec<T extends Comparable<T>> implements Encoder<ComparableColumnStatistics<T>> {
        private Encoder<T> valueEncoder;
        private DistributionFactory<T> distributionFactory;

        public EncDec(Encoder<T> valueEncoder,DistributionFactory<T> distributionFactory) {
            this.valueEncoder = valueEncoder;
            this.distributionFactory = distributionFactory;
        }

        @Override
        public void encode(ComparableColumnStatistics<T> item,DataOutput encoder) throws IOException {
            BaseColumnStatistics.write(item,encoder);
            valueEncoder.encode(item.min, encoder);
            valueEncoder.encode(item.max, encoder);
            CardinalityEstimators.<T>objectEncoder().encode(item.cardinalityEstimator, encoder);
            FrequencyCounters.objectEncoder(valueEncoder).encode(item.frequentElements,encoder);
        }

        @Override
        public ComparableColumnStatistics<T> decode(DataInput decoder) throws IOException {
            int columnId = decoder.readInt();
            long totalBytes = decoder.readLong();
            long totalCount = decoder.readLong();
            long nullCount = decoder.readLong();
            long minCount = decoder.readLong();
            T min = valueEncoder.decode(decoder);
            T max = valueEncoder.decode(decoder);
            CardinalityEstimator<T> cardinalityEstimator = CardinalityEstimators.<T>objectEncoder().decode(decoder);
            FrequentElements<T> frequentElements = FrequencyCounters.objectEncoder(valueEncoder).decode(decoder);
            return new ComparableColumnStatistics<>(columnId,
                    cardinalityEstimator,
                    frequentElements,
                    min,
                    max,
                    totalBytes,
                    totalCount,
                    nullCount,
                    minCount,
                    distributionFactory);
        }
    }
}
