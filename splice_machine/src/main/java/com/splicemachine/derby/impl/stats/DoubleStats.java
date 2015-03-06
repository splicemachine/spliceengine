package com.splicemachine.derby.impl.stats;

import com.google.common.base.Function;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.DoubleColumnStatistics;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.estimate.DoubleDistribution;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.stats.frequency.DoubleFrequencyEstimate;
import com.splicemachine.stats.frequency.DoubleFrequentElements;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLDouble;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class DoubleStats extends BaseDvdStatistics{
    private DoubleColumnStatistics stats;

    public DoubleStats(){}

    public DoubleStats(DoubleColumnStatistics build) {
        super(build);
        stats = build;
    }

    @Override
    public FrequentElements<DataValueDescriptor> topK() {
        return new DoubleFreqs((DoubleFrequentElements) stats.topK());
    }

    @Override public DataValueDescriptor minValue() {
        try {
            return new SQLDouble(stats.min());
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Distribution<DataValueDescriptor> newDistribution(ColumnStatistics baseStats) {
        return new DoubleDist(stats);
    }

    @Override
    public DataValueDescriptor maxValue() {
        try {
            return new SQLDouble(stats.max());
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ColumnStatistics<DataValueDescriptor> getClone() {
        return new DoubleStats((DoubleColumnStatistics)stats.getClone());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        DoubleColumnStatistics.encoder().encode(stats,out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        baseStats = stats = DoubleColumnStatistics.encoder().decode(in);
    }
    /* ****************************************************************************************************************/
    /*private helper methods*/
    private class DoubleFreqs implements FrequentElements<DataValueDescriptor> {
        private DoubleFrequentElements frequentElements;

        public DoubleFreqs(DoubleFrequentElements freqs) {
            this.frequentElements = freqs;
        }

        @Override
        public FrequentElements<DataValueDescriptor> getClone() {
            return new DoubleFreqs((DoubleFrequentElements)frequentElements.getClone());
        }

        @Override
        @SuppressWarnings("unchecked")
        public Set<? extends FrequencyEstimate<DataValueDescriptor>> allFrequentElements() {
            return convert((Set<DoubleFrequencyEstimate>)frequentElements.allFrequentElements());
        }

        @Override public long totalFrequentElements() { return frequentElements.totalFrequentElements(); }

        @Override
        public FrequencyEstimate<? extends DataValueDescriptor> equal(DataValueDescriptor element) {
            try {
                return new DoubleFreq(frequentElements.countEqual(element.getDouble()));
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
                    else return convert(frequentElements.frequentBefore(stop.getDouble(), includeStop));
                }else if(stop==null)
                    return convert(frequentElements.frequentAfter(start.getDouble(),includeStart));
                else
                    return convert(frequentElements.frequentBetween(start.getDouble(),stop.getDouble(),includeStart,includeStop));
            }catch(StandardException se){
                throw new RuntimeException(se); //shouldn't happen
            }
        }

        @Override
        public FrequentElements<DataValueDescriptor> merge(FrequentElements<DataValueDescriptor> other) {
            assert other instanceof DoubleFreqs : "Cannot merge FrequentElements of type " + other.getClass();
            frequentElements = frequentElements.merge(((DoubleFreqs) other).frequentElements);
            return this;
        }

        private Set<? extends FrequencyEstimate<DataValueDescriptor>> convert(Set<DoubleFrequencyEstimate> other) {
            return new ConvertingSetView<>(other,conversionFunction);
        }
    }

    private static class DoubleFreq implements FrequencyEstimate<DataValueDescriptor> {
        private DoubleFrequencyEstimate baseEstimate;

        public DoubleFreq(DoubleFrequencyEstimate intFrequencyEstimate) {
            this.baseEstimate = intFrequencyEstimate;
        }

        @Override public DataValueDescriptor getValue() {
            try {
                return new SQLDouble(baseEstimate.value());
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
        @Override public long count() { return baseEstimate.count(); }
        @Override public long error() { return baseEstimate.error(); }

        @Override
        public FrequencyEstimate<DataValueDescriptor> merge(FrequencyEstimate<DataValueDescriptor> other) {
            assert other instanceof DoubleFreq: "Cannot merge FrequencyEstimate of type "+ other.getClass();
            baseEstimate = (DoubleFrequencyEstimate)baseEstimate.merge(((DoubleFreq) other).baseEstimate);
            return this;
        }

        @Override public String toString() { return baseEstimate.toString(); }
    }

    private static final Function<DoubleFrequencyEstimate,FrequencyEstimate<DataValueDescriptor>> conversionFunction
            = new Function<DoubleFrequencyEstimate, FrequencyEstimate<DataValueDescriptor>>() {
        @Override
        public FrequencyEstimate<DataValueDescriptor> apply(DoubleFrequencyEstimate intFrequencyEstimate) {
            return new DoubleFreq(intFrequencyEstimate);
        }
    };

    private static class DoubleDist implements Distribution<DataValueDescriptor> {
        private DoubleColumnStatistics stats;

        public DoubleDist(DoubleColumnStatistics stats) {
            this.stats = stats;
        }

        @Override
        public long selectivity(DataValueDescriptor element) {
            if(element==null || element.isNull())
                return stats.nullCount();
            double d = safeGetDouble(element);
            return ((DoubleDistribution)stats.getDistribution()).selectivity(d);
        }

        @Override
        public long rangeSelectivity(DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop) {
            if(start==null||start.isNull()){
                if(stop==null||stop.isNull())
                    return stats.getDistribution().rangeSelectivity(null,null,includeStart,includeStop);
                else
                    return ((DoubleDistribution)stats.getDistribution()).selectivityBefore(safeGetDouble(stop),includeStop);
            }else{
                double s = safeGetDouble(start);
                if(stop==null||stop.isNull())
                    return ((DoubleDistribution)stats.getDistribution()).selectivityAfter(s,includeStart);
                else {
                    double e=  safeGetDouble(stop);
                    return ((DoubleDistribution)stats.getDistribution()).rangeSelectivity(s,e,includeStart,includeStop);
                }
            }
        }
    }

    private static double safeGetDouble(DataValueDescriptor dvd){
        try {
            return dvd.getDouble();
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }
}
