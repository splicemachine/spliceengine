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
import com.splicemachine.encoding.Encoder;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.ComparableColumnStatistics;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.estimate.DistributionFactory;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;

import java.io.*;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public abstract class StringStatistics extends BaseDvdStatistics{
    private ColumnStatistics<String> stats;
    protected int strLen;
    private DistributionFactory<String> distributionFactory;

    public StringStatistics() { }

    public StringStatistics(ColumnStatistics<String> stats,int strLen) {
        super(stats);
        this.stats = stats;
        this.strLen = strLen;
        this.distributionFactory = DvdStatsCollector.stringDistributionFactory(strLen);
    }

    @Override
    public FrequentElements<DataValueDescriptor> topK() {
        return new Freqs(stats.topK(),conversionFunction());
    }

    @Override public DataValueDescriptor minValue() { return getDvd(stats.minValue()); }
    @Override public DataValueDescriptor maxValue() { return getDvd(stats.maxValue()); }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(strLen);
        ComparableColumnStatistics.encoder(stringEncoder,distributionFactory).encode((ComparableColumnStatistics<String>) stats,out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        strLen = in.readInt();
        distributionFactory = DvdStatsCollector.stringDistributionFactory(strLen);
        baseStats = stats = ComparableColumnStatistics.encoder(stringEncoder,distributionFactory).decode(in);
    }

    @Override
    protected Distribution<DataValueDescriptor> newDistribution(ColumnStatistics baseStats) {
        return new StringDistribution(baseStats);
    }

    public int maxLength(){
        return strLen;
    }

    protected abstract DataValueDescriptor getDvd(String s);

    protected abstract Function<FrequencyEstimate<String>,FrequencyEstimate<DataValueDescriptor>> conversionFunction();

    /* ****************************************************************************************************************/
    /*private helper methods*/
    public static class Freqs implements FrequentElements<DataValueDescriptor> {
        protected FrequentElements<String> frequentElements;
        private Function<FrequencyEstimate<String>, FrequencyEstimate<DataValueDescriptor>> conversionFunction;

        public Freqs(FrequentElements<String> freqs,Function<FrequencyEstimate<String>,FrequencyEstimate<DataValueDescriptor>> conversionFunction) {
            this.frequentElements = freqs;
            this.conversionFunction = conversionFunction;
        }

        @Override public long totalFrequentElements() { return frequentElements.totalFrequentElements(); }

        @Override
        @SuppressWarnings("unchecked")
        public Set<? extends FrequencyEstimate<DataValueDescriptor>> allFrequentElements() {
            return convert((Set<FrequencyEstimate<String>>)frequentElements.allFrequentElements());
        }

        @Override
        public FrequentElements<DataValueDescriptor> getClone() {
            return new Freqs(frequentElements.getClone(),conversionFunction);
        }

        @Override
        @SuppressWarnings("unchecked")
        public FrequencyEstimate<? extends DataValueDescriptor> equal(DataValueDescriptor element) {
            //TODO -sf- deal with nulls here
            try {
                return conversionFunction.apply((FrequencyEstimate<String>) frequentElements.equal((String)element.getObject()));
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Set<? extends FrequencyEstimate<DataValueDescriptor>> frequentElementsBetween(
                DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop) {
            String startBd;
            String stopBd;
            if(start==null || start.isNull())
                startBd = null;
            else startBd = safeGetString(start);
            if(stop==null || stop.isNull())
                stopBd = null;
            else
                stopBd = safeGetString(stop);
            return convert(frequentElements.frequentElementsBetween(startBd,stopBd,includeStart,includeStop));
        }

        @Override
        public FrequentElements<DataValueDescriptor> merge(FrequentElements<DataValueDescriptor> other) {
            assert other instanceof Freqs : "Cannot merge FrequentElements of type " + other.getClass();
            frequentElements = frequentElements.merge(((Freqs) other).frequentElements);
            return this;
        }

        private Set<? extends FrequencyEstimate<DataValueDescriptor>> convert(Set<? extends FrequencyEstimate<String>> other) {
            return new ConvertingSetView<>(other,conversionFunction);
        }

        protected String safeGetString(DataValueDescriptor element) {
            try {
                return element.getString();
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected static abstract class Freq implements FrequencyEstimate<DataValueDescriptor> {
        private FrequencyEstimate<String> baseEstimate;

        public Freq(FrequencyEstimate<String> intFrequencyEstimate) {
            this.baseEstimate = intFrequencyEstimate;
        }

        @Override public DataValueDescriptor getValue() { return getDvd(baseEstimate.getValue()); }
        @Override public long count() { return baseEstimate.count(); }
        @Override public long error() { return baseEstimate.error(); }

        @Override
        public FrequencyEstimate<DataValueDescriptor> merge(FrequencyEstimate<DataValueDescriptor> other) {
            assert other instanceof Freq: "Cannot merge FrequencyEstimate of type "+ other.getClass();
            baseEstimate = baseEstimate.merge(((Freq) other).baseEstimate);
            return this;
        }

        protected abstract DataValueDescriptor getDvd(String value);

        @Override public String toString() { return baseEstimate.toString(); }
    }


    private static final Encoder<String> stringEncoder = new Encoder<String>() {
        @Override
        public void encode(String item, DataOutput dataInput) throws IOException {
            dataInput.writeBoolean(item!=null);
            if (item!=null)
                dataInput.writeUTF(item);
        }

        @Override
        public String decode(DataInput input) throws IOException {
            return input.readBoolean()?input.readUTF():null;
        }
    };

    public class StringDistribution implements Distribution<DataValueDescriptor> {
        private ColumnStatistics<String> stats;

        public StringDistribution(ColumnStatistics<String> stats) {
            this.stats = stats;
        }

        @Override
        public long selectivity(DataValueDescriptor element) {
            if(element==null||element.isNull()) return stats.nullCount();
            String d = safeGetString(element);
            return stats.getDistribution().selectivity(d);
        }

        @Override
        public long rangeSelectivity(DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop) {
            String s;
            String e;
            if(start==null||start.isNull()) s = null;
            else s = safeGetString(start);
            if(stop==null||stop.isNull()) e = null;
            else e = safeGetString(stop);
            return stats.getDistribution().rangeSelectivity(s,e,includeStart,includeStop);
        }

        @Override
        public DataValueDescriptor minValue(){
            return getDvd(stats.minValue());
        }

        @Override
        public long minCount(){
            return stats.minCount();
        }

        @Override
        public DataValueDescriptor maxValue(){
            return getDvd(stats.maxValue());
        }

        @Override
        public long totalCount(){
            return stats.nonNullCount();
        }
    }

    protected String safeGetString(DataValueDescriptor element) {
        try {
            return element.getString();
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }
}
