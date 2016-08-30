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
import com.splicemachine.db.iapi.types.SQLSmallint;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.CombinedShortColumnStatistics;
import com.splicemachine.stats.ShortColumnStatistics;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.estimate.ShortDistribution;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.stats.frequency.ShortFrequencyEstimate;
import com.splicemachine.stats.frequency.ShortFrequentElements;
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

    public SmallintStats(){}

    public SmallintStats(ShortColumnStatistics build) {
        super(build);
        stats = build;
    }

    @Override
    protected Distribution<DataValueDescriptor> newDistribution(ColumnStatistics baseStats) {
        return new ShortDist((ShortColumnStatistics)baseStats);
    }

    @Override
    public FrequentElements<DataValueDescriptor> topK() {
        return new ShortFreqs((ShortFrequentElements) stats.topK());
    }

    @Override public DataValueDescriptor minValue() { return new SQLSmallint(stats.min()); }
    @Override public DataValueDescriptor maxValue() { return new SQLSmallint(stats.max()); }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        CombinedShortColumnStatistics.encoder().encode(stats,out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.baseStats = stats = CombinedShortColumnStatistics.encoder().decode(in);
    }

    @Override
    public ColumnStatistics<DataValueDescriptor> getClone() {
        return new SmallintStats((ShortColumnStatistics)stats.getClone());
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    static class ShortFreqs implements FrequentElements<DataValueDescriptor> {
        private ShortFrequentElements frequentElements;

        public ShortFreqs(ShortFrequentElements freqs) {
            this.frequentElements = freqs;
        }

        @Override
        public FrequentElements<DataValueDescriptor> getClone() {
            return new ShortFreqs(frequentElements.newCopy());
        }

        @Override public long totalFrequentElements() { return frequentElements.totalFrequentElements(); }

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

    public static class ShortFreq implements FrequencyEstimate<DataValueDescriptor> {
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

        @Override public String toString() { return baseEstimate.toString(); }
    }

    private static final Function<ShortFrequencyEstimate,FrequencyEstimate<DataValueDescriptor>> conversionFunction
            = new Function<ShortFrequencyEstimate, FrequencyEstimate<DataValueDescriptor>>() {
        @Override
        public FrequencyEstimate<DataValueDescriptor> apply(ShortFrequencyEstimate intFrequencyEstimate) {
            return new ShortFreq(intFrequencyEstimate);
        }
    };

    public static class ShortDist implements Distribution<DataValueDescriptor> {
        private ShortDistribution distribution;
        private long nullCount;

        public ShortDist(long nullCount,ShortDistribution distribution) {
            this.distribution = distribution;
            this.nullCount = nullCount;
        }

        public ShortDist(ShortColumnStatistics stats) {
            this.nullCount = stats.nullCount();
            this.distribution = (ShortDistribution)stats.getDistribution();
        }

        @Override
        public long selectivity(DataValueDescriptor element) {
            if(element==null||element.isNull())
                return nullCount;
            short s = safeGetShort(element);
            return distribution.selectivity(s);
        }

        @Override
        public long rangeSelectivity(DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop) {
            if(start==null||start.isNull()){
                if(stop==null||stop.isNull())
                    return distribution.rangeSelectivity(null,null,includeStart,includeStop);
                else
                    return distribution.selectivityBefore(safeGetShort(stop),includeStop);
            }else{
                short s = safeGetShort(start);
                if(stop==null||stop.isNull())
                    return distribution.selectivityAfter(s,includeStart);
                else{
                    short e = safeGetShort(stop);
                    return distribution.rangeSelectivity(s,e,includeStart,includeStop);
                }
            }
        }

        @Override
        public DataValueDescriptor minValue(){
            return new SQLSmallint(distribution.min());
        }

        @Override
        public long minCount(){
            return distribution.minCount();
        }

        @Override
        public DataValueDescriptor maxValue(){
            return new SQLSmallint(distribution.max());
        }

        @Override
        public long totalCount(){
            return distribution.totalCount();
        }
    }

    private static short safeGetShort(DataValueDescriptor element) {
        try {
            return element.getShort();
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }
}
