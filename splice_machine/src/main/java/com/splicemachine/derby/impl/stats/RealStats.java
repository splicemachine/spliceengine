package com.splicemachine.derby.impl.stats;

import com.google.common.base.Function;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.FloatColumnStatistics;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.stats.frequency.FloatFrequencyEstimate;
import com.splicemachine.stats.frequency.FloatFrequentElements;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLReal;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class RealStats extends BaseDvdStatistics {
    private FloatColumnStatistics stats;
    public RealStats(){}

    public RealStats(FloatColumnStatistics build) {
        super(build);
        stats = build;
    }

    @Override
    public FrequentElements<DataValueDescriptor> topK() {
        return new FloatFreqs((FloatFrequentElements)stats.topK());
    }

    @Override
    public DataValueDescriptor minValue() {
        try {
            return new SQLReal(stats.min());
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataValueDescriptor maxValue() {
        try {
            return new SQLReal(stats.max());
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        FloatColumnStatistics.encoder().encode(stats,out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        baseStats = stats = FloatColumnStatistics.encoder().decode(in);
    }

    @Override
    public ColumnStatistics<DataValueDescriptor> getClone() {
        return new RealStats((FloatColumnStatistics)stats.getClone());
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private class FloatFreqs implements FrequentElements<DataValueDescriptor> {
        private FloatFrequentElements frequentElements;

        public FloatFreqs(FloatFrequentElements freqs) {
            this.frequentElements = freqs;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Set<? extends FrequencyEstimate<DataValueDescriptor>> allFrequentElements() {
            return convert((Set<FloatFrequencyEstimate>)frequentElements.allFrequentElements());
        }

        @Override
        public FrequentElements<DataValueDescriptor> getClone() {
            return new FloatFreqs(frequentElements.newCopy());
        }

        @Override
        public FrequencyEstimate<? extends DataValueDescriptor> equal(DataValueDescriptor element) {
            try {
                return new FloatFreq(frequentElements.countEqual(element.getFloat()));
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
                    else return convert(frequentElements.frequentBefore(stop.getFloat(), includeStop));
                }else if(stop==null)
                    return convert(frequentElements.frequentAfter(start.getFloat(),includeStart));
                else
                    return convert(frequentElements.frequentBetween(start.getFloat(),stop.getFloat(),includeStart,includeStop));
            }catch(StandardException se){
                throw new RuntimeException(se); //shouldn't happen
            }
        }

        @Override
        public FrequentElements<DataValueDescriptor> merge(FrequentElements<DataValueDescriptor> other) {
            assert other instanceof FloatFreqs : "Cannot merge FrequentElements of type " + other.getClass();
            frequentElements = frequentElements.merge(((FloatFreqs) other).frequentElements);
            return this;
        }

        private Set<? extends FrequencyEstimate<DataValueDescriptor>> convert(Set<FloatFrequencyEstimate> other) {
            return new ConvertingSetView<>(other,conversionFunction);
        }
    }

    private static class FloatFreq implements FrequencyEstimate<DataValueDescriptor> {
        private FloatFrequencyEstimate baseEstimate;

        public FloatFreq(FloatFrequencyEstimate intFrequencyEstimate) {
            this.baseEstimate = intFrequencyEstimate;
        }

        @Override public DataValueDescriptor getValue() {
            try {
                return new SQLReal(baseEstimate.value());
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
        @Override public long count() { return baseEstimate.count(); }
        @Override public long error() { return baseEstimate.error(); }

        @Override
        public FrequencyEstimate<DataValueDescriptor> merge(FrequencyEstimate<DataValueDescriptor> other) {
            assert other instanceof FloatFreq: "Cannot merge FrequencyEstimate of type "+ other.getClass();
            baseEstimate = (FloatFrequencyEstimate)baseEstimate.merge(((FloatFreq) other).baseEstimate);
            return this;
        }

        @Override public String toString() { return baseEstimate.toString(); }
    }

    private static final Function<FloatFrequencyEstimate,FrequencyEstimate<DataValueDescriptor>> conversionFunction
            = new Function<FloatFrequencyEstimate, FrequencyEstimate<DataValueDescriptor>>() {
        @Override
        public FrequencyEstimate<DataValueDescriptor> apply(FloatFrequencyEstimate intFrequencyEstimate) {
            return new FloatFreq(intFrequencyEstimate);
        }
    };
}
