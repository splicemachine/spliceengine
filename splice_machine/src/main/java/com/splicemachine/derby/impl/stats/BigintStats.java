/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.stats;

import com.google.common.base.Function;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.LongColumnStatistics;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.estimate.LongDistribution;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.stats.frequency.LongFrequencyEstimate;
import com.splicemachine.stats.frequency.LongFrequentElements;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class BigintStats extends BaseDvdStatistics {
    private LongColumnStatistics stats;
    public BigintStats(){}

    public BigintStats(LongColumnStatistics build) {
        super(build);
        stats = build;
    }

    @Override
    protected Distribution<DataValueDescriptor> newDistribution(ColumnStatistics baseStats) {
        return new LongDist(stats);
    }

    @Override
    public FrequentElements<DataValueDescriptor> topK() {
        return new LongFreqs((LongFrequentElements) stats.topK());
    }

    @Override public DataValueDescriptor minValue() { return new SQLLongint(stats.min()); }
    @Override public DataValueDescriptor maxValue() { return new SQLLongint(stats.max()); }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        LongColumnStatistics.encoder().encode(stats,out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.baseStats = stats = LongColumnStatistics.encoder().decode(in);
    }

    @Override
    public ColumnStatistics<DataValueDescriptor> getClone() {
        return new BigintStats((LongColumnStatistics) stats.getClone());
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    static class LongFreqs implements FrequentElements<DataValueDescriptor> {
        private LongFrequentElements frequentElements;

        public LongFreqs(LongFrequentElements freqs) {
            this.frequentElements = freqs;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Set<? extends FrequencyEstimate<DataValueDescriptor>> allFrequentElements() {
            return convert((Set<LongFrequencyEstimate>)frequentElements.allFrequentElements());
        }

        @Override public long totalFrequentElements() { return frequentElements.totalFrequentElements(); }

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

    private static long safeGetLong(DataValueDescriptor element) {
        try {
            return element.getLong();
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }

    static class LongDist implements Distribution<DataValueDescriptor> {
        private LongColumnStatistics stats;

        public LongDist(LongColumnStatistics stats) {
            this.stats = stats;
        }

        @Override
        public long selectivity(DataValueDescriptor element) {
            if(element==null||element.isNull())
                return stats.nullCount();
            return ((LongDistribution)stats.getDistribution()).selectivity(safeGetLong(element));
        }

        @Override
        public long rangeSelectivity(DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop) {
            LongDistribution dist = (LongDistribution)stats.getDistribution();
            if(start==null||start.isNull()){
                if(stop==null||stop.isNull())
                    return dist.rangeSelectivity(null,null,includeStart,includeStop);
                else
                    return dist.selectivityBefore(safeGetLong(stop),includeStop);
            }else{
                long s = safeGetLong(start);
                if(stop==null||stop.isNull())
                    return dist.selectivityAfter(s,includeStart);
                else
                    return dist.rangeSelectivity(s,safeGetLong(stop),includeStart,includeStop);
            }
        }

        @Override
        public DataValueDescriptor minValue(){
            return new SQLLongint(stats.min());
        }

        @Override
        public long minCount(){
            return stats.minCount();
        }

        @Override
        public DataValueDescriptor maxValue(){
            return new SQLLongint(stats.max());
        }

        @Override
        public long totalCount(){
            return stats.nonNullCount();
        }
    }
}
