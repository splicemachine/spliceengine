package com.splicemachine.derby.impl.stats;

import com.google.common.base.Function;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.LongColumnStatistics;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.stats.frequency.LongFrequencyEstimate;
import com.splicemachine.stats.frequency.LongFrequentElements;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLLongint;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class BigintStats extends BaseDvdStatistics {
    private LongColumnStatistics baseStats;

    public BigintStats(LongColumnStatistics build) {
        baseStats = build;
    }

    @Override
    public FrequentElements<DataValueDescriptor> topK() {
        return new LongFreqs((LongFrequentElements) baseStats.topK());
    }

    @Override public DataValueDescriptor minValue() { return new SQLLongint(baseStats.min()); }
    @Override public DataValueDescriptor maxValue() { return new SQLLongint(baseStats.max()); }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        LongColumnStatistics.encoder().encode(baseStats,out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        LongColumnStatistics.encoder().decode(in);
    }

    @Override
    public ColumnStatistics<DataValueDescriptor> getClone() {
        return new BigintStats((LongColumnStatistics)baseStats.getClone());
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private class LongFreqs implements FrequentElements<DataValueDescriptor> {
        private LongFrequentElements frequentElements;

        public LongFreqs(LongFrequentElements freqs) {
            this.frequentElements = freqs;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Set<? extends FrequencyEstimate<DataValueDescriptor>> allFrequentElements() {
            return convert((Set<LongFrequencyEstimate>)frequentElements.allFrequentElements());
        }

        @Override
        public FrequencyEstimate<? extends DataValueDescriptor> equal(DataValueDescriptor element) {
            try {
                return new LongFreq(frequentElements.countEqual(element.getLong()));
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public FrequentElements<DataValueDescriptor> getClone() {
            return new LongFreqs(frequentElements.newCopy());
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
                    else return convert(frequentElements.frequentBefore(stop.getLong(), includeStop));
                }else if(stop==null)
                    return convert(frequentElements.frequentAfter(start.getLong(),includeStart));
                else
                    return convert(frequentElements.frequentBetween(start.getLong(),stop.getLong(),includeStart,includeStop));
            }catch(StandardException se){
                throw new RuntimeException(se); //shouldn't happen
            }
        }

        @Override
        public FrequentElements<DataValueDescriptor> merge(FrequentElements<DataValueDescriptor> other) {
            assert other instanceof LongFreqs : "Cannot merge FrequentElements of type " + other.getClass();
            frequentElements = frequentElements.merge(((LongFreqs) other).frequentElements);
            return this;
        }

        private Set<? extends FrequencyEstimate<DataValueDescriptor>> convert(Set<LongFrequencyEstimate> other) {
            return new ConvertingSetView<>(other,conversionFunction);
        }
    }

    private static class LongFreq implements FrequencyEstimate<DataValueDescriptor> {
        private LongFrequencyEstimate baseEstimate;

        public LongFreq(LongFrequencyEstimate intFrequencyEstimate) {
            this.baseEstimate = intFrequencyEstimate;
        }

        @Override public DataValueDescriptor getValue() { return new SQLLongint(baseEstimate.value()); }
        @Override public long count() { return baseEstimate.count(); }
        @Override public long error() { return baseEstimate.error(); }

        @Override
        public FrequencyEstimate<DataValueDescriptor> merge(FrequencyEstimate<DataValueDescriptor> other) {
            assert other instanceof LongFreq: "Cannot merge FrequencyEstimate of type "+ other.getClass();
            baseEstimate = (LongFrequencyEstimate)baseEstimate.merge(((LongFreq) other).baseEstimate);
            return this;
        }

        @Override public String toString() { return baseEstimate.toString(); }
    }

    private static final Function<LongFrequencyEstimate,FrequencyEstimate<DataValueDescriptor>> conversionFunction
            = new Function<LongFrequencyEstimate, FrequencyEstimate<DataValueDescriptor>>() {
        @Override
        public FrequencyEstimate<DataValueDescriptor> apply(LongFrequencyEstimate intFrequencyEstimate) {
            return new LongFreq(intFrequencyEstimate);
        }
    };
}
