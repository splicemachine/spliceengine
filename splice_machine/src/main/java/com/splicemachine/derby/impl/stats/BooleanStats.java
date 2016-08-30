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
import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.stats.BooleanColumnStatistics;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.estimate.BooleanDistribution;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.frequency.BooleanFrequencyEstimate;
import com.splicemachine.stats.frequency.BooleanFrequentElements;
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
public class BooleanStats extends BaseDvdStatistics {
    private BooleanColumnStatistics baseStats;

    public BooleanStats() { }

    public BooleanStats(BooleanColumnStatistics baseStats) {
        super(baseStats);
        this.baseStats = baseStats;
    }

    @Override
    public FrequentElements<DataValueDescriptor> topK() {
        return new BooleanFreqs((BooleanFrequentElements) baseStats.topK());
    }

    @Override
    public DataValueDescriptor minValue() {
        if(baseStats.trueCount().count()>0)return SQLBoolean.trueTruthValue();
        else if(baseStats.falseCount().count()>0) return SQLBoolean.falseTruthValue();
        return SQLBoolean.unknownTruthValue(); //should never happen, but just in case
    }

    @Override
    public DataValueDescriptor maxValue() {
        if(baseStats.falseCount().count()>0)return SQLBoolean.falseTruthValue();
        else if(baseStats.trueCount().count()>0) return SQLBoolean.trueTruthValue();
        return SQLBoolean.unknownTruthValue(); //should never happen, but just in case
    }

    @Override
    protected Distribution<DataValueDescriptor> newDistribution(ColumnStatistics baseStats) {
        return new BooleanDist(baseStats);
    }

    @Override
    public ColumnStatistics<DataValueDescriptor> getClone() {
        return new BooleanStats((BooleanColumnStatistics)baseStats.getClone());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        BooleanColumnStatistics.encoder().encode(baseStats,out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.baseStats = baseStats = BooleanColumnStatistics.encoder().decode(in);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    public static class BooleanFreqs implements FrequentElements<DataValueDescriptor> {
        private BooleanFrequentElements frequentElements;

        public BooleanFreqs(BooleanFrequentElements freqs) {
            this.frequentElements = freqs;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Set<? extends FrequencyEstimate<DataValueDescriptor>> allFrequentElements() {
            return convert((Set<BooleanFrequencyEstimate>)frequentElements.allFrequentElements());
        }

        @Override
        public FrequencyEstimate<? extends DataValueDescriptor> equal(DataValueDescriptor element) {
            try {
                return new BooleanFreq(frequentElements.equals(element.getBoolean()));
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }

        @Override public long totalFrequentElements() { return frequentElements.totalFrequentElements(); }

        @Override
        public FrequentElements<DataValueDescriptor> getClone() {
            return new BooleanFreqs(frequentElements.getClone());
        }

        @Override
        @SuppressWarnings("unchecked")
        public Set<? extends FrequencyEstimate<DataValueDescriptor>> frequentElementsBetween(
                DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop) {
            try {
                Set<BooleanFrequencyEstimate> baseEstimate;
                if (start == null || start.isNull()) {
                    if (stop == null || stop.isNull()) {
                        //get everything
                        baseEstimate = (Set<BooleanFrequencyEstimate>) frequentElements.allFrequentElements();
                    } else {
                        baseEstimate =  (Set<BooleanFrequencyEstimate>)frequentElements.frequentElementsBetween(
                                Boolean.TRUE,stop.getBoolean(),
                                true,includeStop);
                    }
                }else if(stop==null||stop.isNull()) {
                    baseEstimate = (Set<BooleanFrequencyEstimate>)frequentElements.frequentElementsBetween(
                            start.getBoolean(), Boolean.FALSE,
                            includeStart, true);
                }else{
                    baseEstimate = (Set<BooleanFrequencyEstimate>)frequentElements.frequentElementsBetween(
                            start.getBoolean(),start.getBoolean(),
                            includeStart,includeStop);
                }
                return convert(baseEstimate);

            }catch(StandardException se){
                throw new RuntimeException(se); //shouldn't happen
            }
        }

        @Override
        public FrequentElements<DataValueDescriptor> merge(FrequentElements<DataValueDescriptor> other) {
            assert other instanceof BooleanFreqs : "Cannot merge FrequentElements of type " + other.getClass();
            frequentElements = (BooleanFrequentElements)frequentElements.merge(((BooleanFreqs) other).frequentElements);
            return this;
        }

        private Set<? extends FrequencyEstimate<DataValueDescriptor>> convert(Set<BooleanFrequencyEstimate> other) {
            return new ConvertingSetView<>(other,conversionFunction);
        }
    }

    private static boolean safeGetBoolean(DataValueDescriptor element) {
        try {
            return element.getBoolean();
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }

    public  static class BooleanDist implements Distribution<DataValueDescriptor>{
        private ColumnStatistics<Boolean> stats;

        public BooleanDist(ColumnStatistics<Boolean> stats) {
            this.stats = stats;
        }

        @Override
        public long selectivity(DataValueDescriptor element) {
            if(element==null||element.isNull()) return stats.nullCount();
            boolean d = safeGetBoolean(element);
            return ((BooleanDistribution)stats.getDistribution()).selectivity(d);
        }


        @Override
        public long rangeSelectivity(DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop) {
            Boolean s;
            Boolean e;
            if(start==null||start.isNull())
                s = null;
            else //noinspection UnnecessaryBoxing
                s = Boolean.valueOf(safeGetBoolean(start));
            if(stop==null || stop.isNull())
               e = null;
            else {
                //noinspection UnnecessaryBoxing
                e = Boolean.valueOf(safeGetBoolean(stop));
            }

            return stats.getDistribution().rangeSelectivity(s,e,includeStart,includeStop);
        }

        @Override
        public DataValueDescriptor minValue(){
            return new SQLBoolean(stats.minValue());
        }

        @Override
        public long minCount(){
            return stats.minCount();
        }

        @Override
        public DataValueDescriptor maxValue(){
            return new SQLBoolean(stats.maxValue());
        }

        @Override
        public long totalCount(){
            return stats.nonNullCount();
        }
    }

    private static class BooleanFreq implements FrequencyEstimate<DataValueDescriptor> {
        private BooleanFrequencyEstimate baseEstimate;

        public BooleanFreq(BooleanFrequencyEstimate intFrequencyEstimate) {
            this.baseEstimate = intFrequencyEstimate;
        }

        @Override public DataValueDescriptor getValue() {
            return SQLBoolean.truthValue(baseEstimate.value());
        }
        @Override public long count() { return baseEstimate.count(); }
        @Override public long error() { return baseEstimate.error(); }

        @Override
        public FrequencyEstimate<DataValueDescriptor> merge(FrequencyEstimate<DataValueDescriptor> other) {
            assert other instanceof BooleanFreq: "Cannot merge FrequencyEstimate of type "+ other.getClass();
            baseEstimate = (BooleanFrequencyEstimate)baseEstimate.merge(((BooleanFreq) other).baseEstimate);
            return this;
        }

        @Override public String toString(){ return baseEstimate.toString(); }
    }

    private static final Function<BooleanFrequencyEstimate,FrequencyEstimate<DataValueDescriptor>> conversionFunction
            = new Function<BooleanFrequencyEstimate, FrequencyEstimate<DataValueDescriptor>>() {
        @Override
        public FrequencyEstimate<DataValueDescriptor> apply(BooleanFrequencyEstimate intFrequencyEstimate) {
            return new BooleanFreq(intFrequencyEstimate);
        }
    };
}
