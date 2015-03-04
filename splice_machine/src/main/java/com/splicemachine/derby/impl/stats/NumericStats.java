package com.splicemachine.derby.impl.stats;

import com.google.common.base.Function;
import com.splicemachine.encoding.Encoder;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.ComparableColumnStatistics;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLDecimal;

import java.io.*;
import java.math.BigDecimal;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class NumericStats extends BaseDvdStatistics{
    private ColumnStatistics<BigDecimal> stats;

    public NumericStats(){}
    public NumericStats(ColumnStatistics<BigDecimal> stats) {
        super(stats);
        this.stats = stats;
    }

    @Override
    public FrequentElements<DataValueDescriptor> topK() {
        return new Freqs(stats.topK());
    }

    @Override
    public DataValueDescriptor minValue() {
        return new SQLDecimal(stats.minValue());
    }

    @Override
    public DataValueDescriptor maxValue() {
        return new SQLDecimal(stats.maxValue());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeExternal(ObjectOutput out) throws IOException {
        ComparableColumnStatistics.encoder(bigDecimalEncoder).encode((ComparableColumnStatistics<BigDecimal>) stats,out);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        baseStats = stats = ComparableColumnStatistics.encoder(bigDecimalEncoder).decode(in);
    }

    @Override
    public ColumnStatistics<DataValueDescriptor> getClone() {
        return new NumericStats(stats.getClone());
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private class Freqs implements FrequentElements<DataValueDescriptor> {
        private FrequentElements<BigDecimal> frequentElements;

        public Freqs(FrequentElements<BigDecimal> freqs) {
            this.frequentElements = freqs;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Set<? extends FrequencyEstimate<DataValueDescriptor>> allFrequentElements() {
            return convert((Set<FrequencyEstimate<BigDecimal>>)frequentElements.allFrequentElements());
        }

        @Override
        public FrequentElements<DataValueDescriptor> getClone() {
            return new Freqs(frequentElements.getClone());
        }

        @Override
        @SuppressWarnings("unchecked")
        public FrequencyEstimate<? extends DataValueDescriptor> equal(DataValueDescriptor element) {
            try {
                return new Freq((FrequencyEstimate<BigDecimal>) frequentElements.equal((BigDecimal)element.getObject()));
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Set<? extends FrequencyEstimate<DataValueDescriptor>> frequentElementsBetween(
                DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop) {
            try {
                BigDecimal startBd;
                BigDecimal stopBd;
                if (start == null || start.isNull()) {
                    if (stop == null||stop.isNull()) {
                        //get everything
                        return allFrequentElements();
                    } else{
                        startBd = null;
                        stopBd = (BigDecimal)stop.getObject();
                    }
                }else if(stop==null||stop.isNull()){
                    stopBd = null;
                    startBd = (BigDecimal)start.getObject();
                } else{
                    startBd = (BigDecimal)start.getObject();
                    stopBd = (BigDecimal)stop.getObject();
                }
                return convert(frequentElements.frequentElementsBetween(startBd,stopBd,includeStart,includeStop));
            }catch(StandardException se){
                throw new RuntimeException(se); //shouldn't happen
            }
        }

        @Override
        public FrequentElements<DataValueDescriptor> merge(FrequentElements<DataValueDescriptor> other) {
            assert other instanceof Freqs : "Cannot merge FrequentElements of type " + other.getClass();
            frequentElements = frequentElements.merge(((Freqs) other).frequentElements);
            return this;
        }

        private Set<? extends FrequencyEstimate<DataValueDescriptor>> convert(Set<? extends FrequencyEstimate<BigDecimal>> other) {
            return new ConvertingSetView<>(other,conversionFunction);
        }
    }

    private static class Freq implements FrequencyEstimate<DataValueDescriptor> {
        private FrequencyEstimate<BigDecimal> baseEstimate;

        public Freq(FrequencyEstimate<BigDecimal> intFrequencyEstimate) {
            this.baseEstimate = intFrequencyEstimate;
        }

        @Override public DataValueDescriptor getValue() { return new SQLDecimal(baseEstimate.getValue()); }
        @Override public long count() { return baseEstimate.count(); }
        @Override public long error() { return baseEstimate.error(); }

        @Override
        public FrequencyEstimate<DataValueDescriptor> merge(FrequencyEstimate<DataValueDescriptor> other) {
            assert other instanceof Freq: "Cannot merge FrequencyEstimate of type "+ other.getClass();
            baseEstimate = baseEstimate.merge(((Freq) other).baseEstimate);
            return this;
        }

        @Override public String toString() { return baseEstimate.toString(); }
    }

    private static final Function<FrequencyEstimate<BigDecimal>,FrequencyEstimate<DataValueDescriptor>> conversionFunction
            = new Function<FrequencyEstimate<BigDecimal>, FrequencyEstimate<DataValueDescriptor>>() {
        @Override
        public FrequencyEstimate<DataValueDescriptor> apply(FrequencyEstimate<BigDecimal> intFrequencyEstimate) {
            return new Freq(intFrequencyEstimate);
        }
    };

    private static final Encoder<BigDecimal> bigDecimalEncoder = new Encoder<BigDecimal>() {
        @Override
        public void encode(BigDecimal item, DataOutput dataInput) throws IOException {
            byte[] dataEncoding = Encoding.encode(item);
            dataInput.write(dataEncoding.length);
            dataInput.write(dataEncoding);
        }

        @Override
        public BigDecimal decode(DataInput input) throws IOException {
            byte[] data = new byte[input.readInt()];
            input.readFully(data);
            return Encoding.decodeBigDecimal(data);
        }
    };
}
