package com.splicemachine.derby.impl.stats;

import com.google.common.base.Function;
import com.splicemachine.encoding.Encoder;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.ComparableColumnStatistics;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.*;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public abstract class StringStatistics extends BaseDvdStatistics{
    private ColumnStatistics<String> stats;

    public StringStatistics() { }

    public StringStatistics(ColumnStatistics<String> stats) {
        super(stats);
        this.stats = stats;
    }

    @Override
    public FrequentElements<DataValueDescriptor> topK() {
        return new Freqs(stats.topK());
    }

    @Override public DataValueDescriptor minValue() { return getDvd(stats.minValue()); }
    @Override public DataValueDescriptor maxValue() { return getDvd(stats.maxValue()); }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        ComparableColumnStatistics.encoder(stringEncoder).encode((ComparableColumnStatistics<String>) stats,out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        baseStats = stats = ComparableColumnStatistics.encoder(stringEncoder).decode(in);
    }


    protected abstract DataValueDescriptor getDvd(String s);

    protected abstract Function<FrequencyEstimate<String>,FrequencyEstimate<DataValueDescriptor>> conversionFunction();

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private class Freqs implements FrequentElements<DataValueDescriptor> {
        private FrequentElements<String> frequentElements;

        public Freqs(FrequentElements<String> freqs) {
            this.frequentElements = freqs;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Set<? extends FrequencyEstimate<DataValueDescriptor>> allFrequentElements() {
            return convert((Set<FrequencyEstimate<String>>)frequentElements.allFrequentElements());
        }

        @Override
        public FrequentElements<DataValueDescriptor> getClone() {
            return new Freqs(frequentElements.getClone());
        }

        @Override
        @SuppressWarnings("unchecked")
        public FrequencyEstimate<? extends DataValueDescriptor> equal(DataValueDescriptor element) {
            try {
                return conversionFunction().apply((FrequencyEstimate<String>) frequentElements.equal((String)element.getObject()));
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Set<? extends FrequencyEstimate<DataValueDescriptor>> frequentElementsBetween(
                DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop) {
            try {
                String startBd;
                String stopBd;
                if (start == null || start.isNull()) {
                    if (stop == null||stop.isNull()) {
                        //get everything
                        return allFrequentElements();
                    } else{
                        startBd = null;
                        stopBd = stop.getString();
                    }
                }else if(stop==null||stop.isNull()){
                    stopBd = null;
                    startBd = start.getString();
                } else{
                    startBd = start.getString();
                    stopBd = stop.getString();
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

        private Set<? extends FrequencyEstimate<DataValueDescriptor>> convert(Set<? extends FrequencyEstimate<String>> other) {
            return new ConvertingSetView<>(other,conversionFunction());
        }
    }

    protected static abstract class Freq implements FrequencyEstimate<DataValueDescriptor> {
        private FrequencyEstimate<String> baseEstimate;

        public Freq(FrequencyEstimate<String> intFrequencyEstimate) {
            this.baseEstimate = intFrequencyEstimate;
        }

        @Override public DataValueDescriptor getValue() { return getDvd(baseEstimate.getValue()); }
        @Override public long count() { return baseEstimate.count(); }
        @Override public long error() { return baseEstimate.error(); }

        @Override
        public FrequencyEstimate<DataValueDescriptor> merge(FrequencyEstimate<DataValueDescriptor> other) {
            assert other instanceof Freq: "Cannot merge FrequencyEstimate of type "+ other.getClass();
            baseEstimate = baseEstimate.merge(((Freq) other).baseEstimate);
            return this;
        }

        protected abstract DataValueDescriptor getDvd(String value);

        @Override public String toString() { return baseEstimate.toString(); }
    }


    private static final Encoder<String> stringEncoder = new Encoder<String>() {
        @Override
        public void encode(String item, DataOutput dataInput) throws IOException {
            dataInput.writeUTF(item);
        }

        @Override
        public String decode(DataInput input) throws IOException {
            return input.readUTF();
        }
    };
}
