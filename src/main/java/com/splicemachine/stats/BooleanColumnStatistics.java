package com.splicemachine.stats;

import com.splicemachine.encoding.Encoder;
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
public class BooleanColumnStatistics implements ColumnStatistics<Boolean> {
    private BooleanFrequentElements frequentElements;

    private long totalBytes;
    private long totalCount;
    private long nullCount;

    public BooleanColumnStatistics( BooleanFrequentElements frequentElements,
                                  long totalBytes,
                                  long totalCount,
                                  long nullCount) {
        this.frequentElements = frequentElements;
        this.totalBytes = totalBytes;
        this.totalCount = totalCount;
        this.nullCount = nullCount;
    }

    @Override public long cardinality() {
        long c = 0;
        if(frequentElements.equalsTrue().count()>0) c++;
        if(frequentElements.equalsFalse().count()>0) c++;
        if(nullCount()>0)c++;
        return c;
    }

    @Override public float nullFraction() { return ((float)nullCount)/totalCount; }
    @Override public long nullCount() { return nullCount; }
    @Override public FrequentElements<Boolean> topK() { return frequentElements; }
    @Override public Boolean minValue() { return Boolean.TRUE; }
    @Override public Boolean maxValue() { return Boolean.FALSE; }
    public boolean min(){ return true; }
    public boolean max(){ return false; }
    @Override public long avgColumnWidth() { return totalBytes/totalCount;}

    @Override
    public ColumnStatistics<Boolean> merge(ColumnStatistics<Boolean> other) {
        assert other instanceof BooleanColumnStatistics: "Cannot merge statistics of type "+ other.getClass();
        BooleanColumnStatistics o = (BooleanColumnStatistics)other;
        frequentElements = frequentElements.merge(o.frequentElements);
        totalBytes+=o.totalBytes;
        totalCount+=o.totalCount;
        nullCount+=o.nullCount;
        return this;
    }

    static class EncDec implements Encoder<BooleanColumnStatistics> {
        public static final EncDec INSTANCE = new EncDec();

        @Override
        public void encode(BooleanColumnStatistics item,DataOutput encoder) throws IOException {
            encoder.writeLong(item.totalBytes);
            encoder.writeLong(item.totalCount);
            encoder.writeLong(item.nullCount);
            FrequencyCounters.booleanEncoder().encode(item.frequentElements,encoder);
        }

        @Override
        public BooleanColumnStatistics decode(DataInput decoder) throws IOException {
            long totalBytes = decoder.readLong();
            long totalCount = decoder.readLong();
            long nullCount = decoder.readLong();
            BooleanFrequentElements frequentElements = FrequencyCounters.booleanEncoder().decode(decoder);
            return new BooleanColumnStatistics(frequentElements,totalBytes,totalCount,nullCount);
        }
    }
}

