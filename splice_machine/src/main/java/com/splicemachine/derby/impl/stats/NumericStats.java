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
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.encoding.Encoder;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.ComparableColumnStatistics;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.estimate.DistributionFactory;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;

import java.io.*;
import java.math.BigDecimal;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class NumericStats extends BaseDvdStatistics{
    private ColumnStatistics<BigDecimal> stats;
    private DistributionFactory distributionFactory;

    public NumericStats(){
        this.distributionFactory = DvdStatsCollector.decimalDistributionFactory;
    }

    public NumericStats(ColumnStatistics<BigDecimal> stats){
        super(stats);
        this.stats = stats;
        this.distributionFactory = DvdStatsCollector.decimalDistributionFactory;

    }

    @Override public FrequentElements<DataValueDescriptor> topK() { return new Freqs(stats.topK()); }
    @Override public DataValueDescriptor minValue() { return new SQLDecimal(stats.minValue()); }
    @Override public DataValueDescriptor maxValue() { return new SQLDecimal(stats.maxValue()); }

    @Override
    @SuppressWarnings("unchecked")
    public void writeExternal(ObjectOutput out) throws IOException {
        ComparableColumnStatistics.encoder(bigDecimalEncoder,distributionFactory).encode(stats,out);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        baseStats = stats = ComparableColumnStatistics.encoder(bigDecimalEncoder,DvdStatsCollector.decimalDistributionFactory).decode(in);
    }

    @Override
    public ColumnStatistics<DataValueDescriptor> getClone() {
        return new NumericStats(stats.getClone());
    }

    @Override
    protected Distribution<DataValueDescriptor> newDistribution(ColumnStatistics baseStats) {
        return new NumericDistribution(stats);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    static class Freqs implements FrequentElements<DataValueDescriptor> {
        private FrequentElements<BigDecimal> frequentElements;

        public Freqs(FrequentElements<BigDecimal> freqs) {
            this.frequentElements = freqs;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Set<? extends FrequencyEstimate<DataValueDescriptor>> allFrequentElements() {
            return convert((Set<FrequencyEstimate<BigDecimal>>)frequentElements.allFrequentElements());
        }

        @Override public long totalFrequentElements() { return frequentElements.totalFrequentElements(); }

        @Override
        public FrequentElements<DataValueDescriptor> getClone() {
            return new Freqs(frequentElements.getClone());
        }

        @Override
        @SuppressWarnings("unchecked")
        public FrequencyEstimate<? extends DataValueDescriptor> equal(DataValueDescriptor element) {
            try {
                return new Freq((FrequencyEstimate<BigDecimal>) frequentElements.equal((BigDecimal)element.getObject()));
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Set<? extends FrequencyEstimate<DataValueDescriptor>> frequentElementsBetween(
                DataValueDescriptor start, DataValueDescriptor stop,
                boolean includeStart, boolean includeStop) {
            BigDecimal startBd;
            BigDecimal stopBd;
            if(start==null || start.isNull())
                startBd = null;
            else
                startBd = safeGetDecimal(start);
            if(stop==null || stop.isNull())
                stopBd = null;
            else
                stopBd = safeGetDecimal(stop);
            return convert(frequentElements.frequentElementsBetween(startBd,stopBd,includeStart,includeStop));
        }

        @Override
        public FrequentElements<DataValueDescriptor> merge(FrequentElements<DataValueDescriptor> other) {
            assert other instanceof Freqs : "Cannot merge FrequentElements of type " + other.getClass();
            frequentElements = frequentElements.merge(((Freqs) other).frequentElements);
            return this;
        }

        private Set<? extends FrequencyEstimate<DataValueDescriptor>> convert(Set<? extends FrequencyEstimate<BigDecimal>> other) {
            return new ConvertingSetView<>(other,conversionFunction);
        }
    }

    private static BigDecimal safeGetDecimal(DataValueDescriptor value) {
        try {
            return ((NumberDataValue)value).getBigDecimal();
        } catch (StandardException se) {
            throw new RuntimeException(se);
        }
    }

    private static class Freq implements FrequencyEstimate<DataValueDescriptor> {
        private FrequencyEstimate<BigDecimal> baseEstimate;

        public Freq(FrequencyEstimate<BigDecimal> intFrequencyEstimate) {
            this.baseEstimate = intFrequencyEstimate;
        }

        @Override public DataValueDescriptor getValue() { return new SQLDecimal(baseEstimate.getValue()); }
        @Override public long count() { return baseEstimate.count(); }
        @Override public long error() { return baseEstimate.error(); }

        @Override
        public FrequencyEstimate<DataValueDescriptor> merge(FrequencyEstimate<DataValueDescriptor> other) {
            assert other instanceof Freq: "Cannot merge FrequencyEstimate of type "+ other.getClass();
            baseEstimate = baseEstimate.merge(((Freq) other).baseEstimate);
            return this;
        }

        @Override public String toString() { return baseEstimate.toString(); }
    }

    private static final Function<FrequencyEstimate<BigDecimal>,FrequencyEstimate<DataValueDescriptor>> conversionFunction
            = new Function<FrequencyEstimate<BigDecimal>, FrequencyEstimate<DataValueDescriptor>>() {
        @Override
        public FrequencyEstimate<DataValueDescriptor> apply(FrequencyEstimate<BigDecimal> intFrequencyEstimate) {
            return new Freq(intFrequencyEstimate);
        }
    };

    private static final Encoder<BigDecimal> bigDecimalEncoder = new Encoder<BigDecimal>() {
        @Override
        public void encode(BigDecimal item, DataOutput dataInput) throws IOException {
            dataInput.writeBoolean(item!=null);
            if(item!=null){
                byte[] dataEncoding=Encoding.encode(item);
                dataInput.writeInt(dataEncoding.length);
                dataInput.write(dataEncoding);
            }
        }

        @Override
        public BigDecimal decode(DataInput input) throws IOException {
            if(input.readBoolean()){
                byte[] data=new byte[input.readInt()];
                input.readFully(data);
                return Encoding.decodeBigDecimal(data);
            }else
                return null;
        }
    };

    static class NumericDistribution implements Distribution<DataValueDescriptor> {
        private ColumnStatistics<BigDecimal> baseStats;

        public NumericDistribution(ColumnStatistics<BigDecimal> baseStats) {
            this.baseStats = baseStats;
        }

        @Override
        public long selectivity(DataValueDescriptor element) {
            if(element==null ||element.isNull()) return baseStats.nullCount();
            try {
                return baseStats.getDistribution().selectivity((BigDecimal)element.getObject());
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public long rangeSelectivity(DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop) {
            BigDecimal s;
            BigDecimal e;
            if(start==null||start.isNull())
                s = null;
            else
                s =safeGetDecimal(start);
            if(stop==null || stop.isNull())
                e = null;
            else
                e = safeGetDecimal(stop);

            return baseStats.getDistribution().rangeSelectivity(s, e, includeStart, includeStop);
        }

        @Override
        public DataValueDescriptor minValue(){
            return new SQLDecimal(baseStats.minValue());
        }

        @Override
        public long minCount(){
            return baseStats.minCount();
        }

        @Override
        public DataValueDescriptor maxValue(){
            return new SQLDecimal(baseStats.maxValue());
        }

        @Override
        public long totalCount(){
            return baseStats.nonNullCount();
        }
    }
}
