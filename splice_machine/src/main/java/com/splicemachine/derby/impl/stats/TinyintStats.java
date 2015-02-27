package com.splicemachine.derby.impl.stats;

import com.google.common.base.Function;
import com.splicemachine.stats.ByteColumnStatistics;
import com.splicemachine.stats.frequency.ByteFrequencyEstimate;
import com.splicemachine.stats.frequency.ByteFrequentElements;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLTinyint;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class TinyintStats extends BaseDvdStatistics {
    private ByteColumnStatistics stats;

    public TinyintStats(ByteColumnStatistics build) {
        super(build);
        this.stats = build;
    }

    @Override
    public FrequentElements<DataValueDescriptor> topK() {
        return new TinyintFreqs((ByteFrequentElements) stats.topK());
    }

    @Override public DataValueDescriptor minValue() { return new SQLTinyint(stats.min()); }
    @Override public DataValueDescriptor maxValue() { return new SQLTinyint(stats.max()); }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        ByteColumnStatistics.encoder().encode(stats,out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        stats = ByteColumnStatistics.encoder().decode(in);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private class TinyintFreqs implements FrequentElements<DataValueDescriptor> {
        private ByteFrequentElements frequentElements;

        public TinyintFreqs(ByteFrequentElements freqs) {
            this.frequentElements = freqs;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Set<? extends FrequencyEstimate<DataValueDescriptor>> allFrequentElements() {
            return convert((Set<ByteFrequencyEstimate>)frequentElements.allFrequentElements());
        }

        @Override
        public FrequencyEstimate<? extends DataValueDescriptor> equal(DataValueDescriptor element) {
            try {
                return new TinyFreq(frequentElements.countEqual(element.getByte()));
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Set<? extends FrequencyEstimate<DataValueDescriptor>> frequentElementsBetween(
                DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop) {
            try {
                if (start == null) {
                    if (stop == null) {
                        //get everything
                        return allFrequentElements();
                    }
                    else return convert(frequentElements.frequentBefore(stop.getByte(), includeStop));
                }else if(stop==null)
                    return convert(frequentElements.frequentAfter(start.getByte(),includeStart));
                else
                    return convert(frequentElements.frequentBetween(start.getByte(),stop.getByte(),includeStart,includeStop));
            }catch(StandardException se){
                throw new RuntimeException(se); //shouldn't happen
            }
        }

        @Override
        public FrequentElements<DataValueDescriptor> merge(FrequentElements<DataValueDescriptor> other) {
            assert other instanceof TinyintFreqs : "Cannot merge FrequentElements of type " + other.getClass();
            frequentElements = (ByteFrequentElements) frequentElements.merge(((TinyintFreqs) other).frequentElements);
            return this;
        }

        private Set<? extends FrequencyEstimate<DataValueDescriptor>> convert(Set<ByteFrequencyEstimate> other) {
            return new ConvertingSetView<>(other,conversionFunction);
        }
    }

    private static class TinyFreq implements FrequencyEstimate<DataValueDescriptor> {
        private ByteFrequencyEstimate baseEstimate;

        public TinyFreq(ByteFrequencyEstimate intFrequencyEstimate) {
            this.baseEstimate = intFrequencyEstimate;
        }

        @Override public DataValueDescriptor getValue() { return new SQLTinyint(baseEstimate.value()); }
        @Override public long count() { return baseEstimate.count(); }
        @Override public long error() { return baseEstimate.error(); }

        @Override
        public FrequencyEstimate<DataValueDescriptor> merge(FrequencyEstimate<DataValueDescriptor> other) {
            assert other instanceof TinyFreq: "Cannot merge FrequencyEstimate of type "+ other.getClass();
            baseEstimate = (ByteFrequencyEstimate)baseEstimate.merge(((TinyFreq) other).baseEstimate);
            return this;
        }
    }

    private static final Function<ByteFrequencyEstimate,FrequencyEstimate<DataValueDescriptor>> conversionFunction
            = new Function<ByteFrequencyEstimate, FrequencyEstimate<DataValueDescriptor>>() {
        @Override
        public FrequencyEstimate<DataValueDescriptor> apply(ByteFrequencyEstimate intFrequencyEstimate) {
            return new TinyFreq(intFrequencyEstimate);
        }
    };
}
