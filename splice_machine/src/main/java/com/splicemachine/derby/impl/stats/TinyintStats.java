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
import com.splicemachine.db.iapi.types.SQLTinyint;
import com.splicemachine.stats.ByteColumnStatistics;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.estimate.ByteDistribution;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.frequency.ByteFrequencyEstimate;
import com.splicemachine.stats.frequency.ByteFrequentElements;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;

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

    public TinyintStats(){ }

    public TinyintStats(ByteColumnStatistics build) {
        super(build);
        this.stats = build;
    }

    @Override
    protected Distribution<DataValueDescriptor> newDistribution(ColumnStatistics baseStats) {
        return new ByteDist(stats);
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

    @Override
    public ColumnStatistics<DataValueDescriptor> getClone() {
        return new TinyintStats((ByteColumnStatistics)stats.getClone());
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private class TinyintFreqs implements FrequentElements<DataValueDescriptor> {
        private ByteFrequentElements frequentElements;

        public TinyintFreqs(ByteFrequentElements freqs) {
            this.frequentElements = freqs;
        }

        @Override
        public FrequentElements<DataValueDescriptor> getClone() {
            return new TinyintFreqs(frequentElements.getClone());
        }

        @Override public long totalFrequentElements() { return frequentElements.totalFrequentElements(); }

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

    private static class ByteDist implements Distribution<DataValueDescriptor> {
        private ByteColumnStatistics stats;

        public ByteDist(ByteColumnStatistics stats) {
            this.stats = stats;
        }

        @Override
        public long selectivity(DataValueDescriptor element) {
            if(element==null||element.isNull())
                return stats.nullCount();
            return ((ByteDistribution)stats.getDistribution()).selectivity(safeGetByte(element));
        }

        @Override
        public long rangeSelectivity(DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop) {
            ByteDistribution dist = (ByteDistribution)stats.getDistribution();
            if(start==null||start.isNull()){
                if(stop==null||stop.isNull()){
                    return dist.rangeSelectivity(null,null,includeStart,includeStop);
                }else{
                    return dist.selectivityBefore(safeGetByte(stop),includeStop);
                }
            }else{
                byte s = safeGetByte(start);
                if(stop==null||stop.isNull())
                    return dist.selectivityAfter(s,includeStart);
                else
                    return dist.rangeSelectivity(s,safeGetByte(stop),includeStart,includeStop);
            }
        }

        @Override
        public DataValueDescriptor minValue(){
            return new SQLTinyint(stats.min());
        }

        @Override
        public long minCount(){
            return stats.minCount();
        }

        @Override
        public DataValueDescriptor maxValue(){
            return new SQLTinyint(stats.max());
        }

        @Override
        public long totalCount(){
            return stats.nonNullCount();
        }
    }

    private static byte safeGetByte(DataValueDescriptor element) {
        try {
            return element.getByte();
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }
}
