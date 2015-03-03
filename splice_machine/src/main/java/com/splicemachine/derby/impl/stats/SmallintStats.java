package com.splicemachine.derby.impl.stats;

import com.google.common.base.Function;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.ShortColumnStatistics;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.stats.frequency.ShortFrequencyEstimate;
import com.splicemachine.stats.frequency.ShortFrequentElements;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLSmallint;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class SmallintStats extends BaseDvdStatistics {
    private ShortColumnStatistics stats;

    public SmallintStats(ShortColumnStatistics build) {
        stats = build;
    }

    @Override
    public FrequentElements<DataValueDescriptor> topK() {
        return new ShortFreqs((ShortFrequentElements) stats.topK());
    }

    @Override public DataValueDescriptor minValue() { return new SQLSmallint(stats.min()); }
    @Override public DataValueDescriptor maxValue() { return new SQLSmallint(stats.max()); }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        ShortColumnStatistics.encoder().encode(stats,out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        stats = ShortColumnStatistics.encoder().decode(in);
    }

    @Override
    public ColumnStatistics<DataValueDescriptor> getClone() {
        return new SmallintStats((ShortColumnStatistics)stats.getClone());
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private class ShortFreqs implements FrequentElements<DataValueDescriptor> {
        private ShortFrequentElements frequentElements;

        public ShortFreqs(ShortFrequentElements freqs) {
            this.frequentElements = freqs;
        }

        @Override
        public FrequentElements<DataValueDescriptor> getClone() {
            return new ShortFreqs(frequentElements.newCopy());
        }

        @Override
        @SuppressWarnings("unchecked")
        public Set<? extends FrequencyEstimate<DataValueDescriptor>> allFrequentElements() {
            return convert((Set<ShortFrequencyEstimate>)frequentElements.allFrequentElements());
        }

        @Override
        public FrequencyEstimate<? extends DataValueDescriptor> equal(DataValueDescriptor element) {
            try {
                return new ShortFreq(frequentElements.countEqual(element.getShort()));
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
                    else return convert(frequentElements.frequentBefore(stop.getShort(), includeStop));
                }else if(stop==null)
                    return convert(frequentElements.frequentAfter(start.getShort(),includeStart));
                else
                    return convert(frequentElements.frequentBetween(start.getShort(),stop.getShort(),includeStart,includeStop));
            }catch(StandardException se){
                throw new RuntimeException(se); //shouldn't happen
            }
        }

        @Override
        public FrequentElements<DataValueDescriptor> merge(FrequentElements<DataValueDescriptor> other) {
            assert other instanceof ShortFreqs : "Cannot merge FrequentElements of type " + other.getClass();
            frequentElements = frequentElements.merge(((ShortFreqs) other).frequentElements);
            return this;
        }

        private Set<? extends FrequencyEstimate<DataValueDescriptor>> convert(Set<ShortFrequencyEstimate> other) {
            return new ConvertingSetView<>(other,conversionFunction);
        }
    }

    private static class ShortFreq implements FrequencyEstimate<DataValueDescriptor> {
        private ShortFrequencyEstimate baseEstimate;

        public ShortFreq(ShortFrequencyEstimate intFrequencyEstimate) {
            this.baseEstimate = intFrequencyEstimate;
        }

        @Override public DataValueDescriptor getValue() { return new SQLSmallint(baseEstimate.value()); }
        @Override public long count() { return baseEstimate.count(); }
        @Override public long error() { return baseEstimate.error(); }

        @Override
        public FrequencyEstimate<DataValueDescriptor> merge(FrequencyEstimate<DataValueDescriptor> other) {
            assert other instanceof ShortFreq: "Cannot merge FrequencyEstimate of type "+ other.getClass();
            baseEstimate = (ShortFrequencyEstimate)baseEstimate.merge(((ShortFreq) other).baseEstimate);
            return this;
        }
    }

    private static final Function<ShortFrequencyEstimate,FrequencyEstimate<DataValueDescriptor>> conversionFunction
            = new Function<ShortFrequencyEstimate, FrequencyEstimate<DataValueDescriptor>>() {
        @Override
        public FrequencyEstimate<DataValueDescriptor> apply(ShortFrequencyEstimate intFrequencyEstimate) {
            return new ShortFreq(intFrequencyEstimate);
        }
    };
}
