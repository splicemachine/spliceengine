package com.splicemachine.stats;

import com.splicemachine.encoding.Encoder;
import com.splicemachine.stats.estimate.BooleanDistribution;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.frequency.BooleanFrequencyEstimate;
import com.splicemachine.stats.frequency.BooleanFrequentElements;
import com.splicemachine.stats.frequency.FrequencyCounters;
import com.splicemachine.stats.frequency.FrequentElements;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public class BooleanColumnStatistics extends BaseColumnStatistics<Boolean> {
    private BooleanFrequentElements frequentElements;
    private Distribution<Boolean> distribution;

    public BooleanColumnStatistics( int columnId,
                                    BooleanFrequentElements frequentElements,
                                    long totalBytes,
                                    long totalCount,
                                    long nullCount,
                                    long minCount) {
        super(columnId,totalBytes,totalCount,nullCount,minCount);
        this.frequentElements = frequentElements;
    }

    @Override public long cardinality() {
        long c = 0;
        if(frequentElements.equalsTrue().count()>0) c++;
        if(frequentElements.equalsFalse().count()>0) c++;
        if(nullCount()>0)c++;
        return c;
    }

    @Override public FrequentElements<Boolean> topK() { return frequentElements; }
    @Override public Boolean minValue() { return Boolean.TRUE; }
    @Override public Boolean maxValue() { return Boolean.FALSE; }

    public BooleanFrequencyEstimate trueCount(){
        return frequentElements.equalsTrue();
    }

    public BooleanFrequencyEstimate falseCount(){
        return frequentElements.equalsTrue();
    }

    @Override
    public ColumnStatistics<Boolean> getClone() {
        return new BooleanColumnStatistics(columnId,frequentElements.getClone(),totalBytes,totalCount,nullCount,minCount);
    }

    @Override
    public Distribution<Boolean> getDistribution() {
        if(distribution==null){
           distribution = new BooleanDistribution(nullCount,this.frequentElements);
        }
        return distribution;
    }

    @Override
    public ColumnStatistics<Boolean> merge(ColumnStatistics<Boolean> other) {
        assert other instanceof BooleanColumnStatistics: "Cannot merge statistics of type "+ other.getClass();
        BooleanColumnStatistics o = (BooleanColumnStatistics)other;
        frequentElements = (BooleanFrequentElements)frequentElements.merge(o.frequentElements);
        totalBytes+=o.totalBytes;
        totalCount+=o.totalCount;
        nullCount+=o.nullCount;
        return this;
    }

    public static Encoder<BooleanColumnStatistics> encoder(){
        return EncDec.INSTANCE;
    }

    static class EncDec implements Encoder<BooleanColumnStatistics> {
        public static final EncDec INSTANCE = new EncDec();

        @Override
        public void encode(BooleanColumnStatistics item,DataOutput encoder) throws IOException {
            BaseColumnStatistics.write(item, encoder);
            FrequencyCounters.booleanEncoder().encode(item.frequentElements,encoder);
        }

        @Override
        public BooleanColumnStatistics decode(DataInput decoder) throws IOException {
            int columnId = decoder.readInt();
            long totalBytes = decoder.readLong();
            long totalCount = decoder.readLong();
            long nullCount = decoder.readLong();
            long minCount = decoder.readLong();
            BooleanFrequentElements frequentElements = FrequencyCounters.booleanEncoder().decode(decoder);
            return new BooleanColumnStatistics(columnId,frequentElements,totalBytes,totalCount,nullCount,minCount);
        }
    }
}

