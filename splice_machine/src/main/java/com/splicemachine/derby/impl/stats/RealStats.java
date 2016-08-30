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
import com.splicemachine.db.iapi.types.SQLReal;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.FloatColumnStatistics;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.estimate.FloatDistribution;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.stats.frequency.FloatFrequencyEstimate;
import com.splicemachine.stats.frequency.FloatFrequentElements;

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
    protected Distribution<DataValueDescriptor> newDistribution(ColumnStatistics baseStats) {
        return new RealDist(stats);
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
    static class FloatFreqs implements FrequentElements<DataValueDescriptor> {
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

        @Override public long totalFrequentElements() { return frequentElements.totalFrequentElements(); }
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

    static class RealDist implements Distribution<DataValueDescriptor> {
        private FloatColumnStatistics stats;

        public RealDist(FloatColumnStatistics build) {
            stats = build;
        }

        @Override
        public long selectivity(DataValueDescriptor element) {
            if(element==null||element.isNull()) return stats.nullCount();
            float e = safeGetFloat(element);
            return ((FloatDistribution)stats.getDistribution()).selectivity(e);
        }

        @Override
        public long rangeSelectivity(DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop) {
            if(start==null||start.isNull()){
                if(stop==null|| stop.isNull())
                    return stats.getDistribution().rangeSelectivity(null,null,includeStart,includeStop);
                else{
                    float s = safeGetFloat(stop);
                    return ((FloatDistribution)stats.getDistribution()).selectivityBefore(s,includeStop);
                }
            }else{
                float s = safeGetFloat(start);
                if(stop==null|| stop.isNull())
                    return ((FloatDistribution)stats.getDistribution()).selectivityAfter(s,includeStart);
                else{
                    float e = safeGetFloat(stop);
                    return ((FloatDistribution)stats.getDistribution()).rangeSelectivity(s,e,includeStart, includeStop);
                }
            }
        }

        @Override
        public DataValueDescriptor minValue(){
            return safeWrap(stats.min());
        }

        @Override
        public long minCount(){
            return stats.minCount();
        }

        @Override
        public DataValueDescriptor maxValue(){
            return safeWrap(stats.max());
        }

        @Override
        public long totalCount(){
            return stats.nonNullCount();
        }
    }

    private static SQLReal safeWrap(float value){
        try{
            return new SQLReal(value);
        }catch(StandardException e){
            /*
             * This should never happen because we only populate statistics
             * from data stored on a table; this data has already passed bounds checking
             * to be stored in a SQLReal already, and thus we should be safe here.
             *
             * Still, just to be safe, rethrow so that the exception ends up somewhere when
             * it inevitably happens due to some programmer (most likely me) screws up.
             */
            throw new RuntimeException(e);
        }
    }
    private static float safeGetFloat(DataValueDescriptor element) {
        try {
            return element.getFloat();
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }
}
