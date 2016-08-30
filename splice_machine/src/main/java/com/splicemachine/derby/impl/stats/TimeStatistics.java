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
import org.sparkproject.guava.collect.Iterators;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
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
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 3/27/15
 */
public abstract class TimeStatistics extends BaseDvdStatistics{
    protected LongColumnStatistics baseStats;

    public TimeStatistics(){ }

    public TimeStatistics(LongColumnStatistics baseStats){
        super(baseStats);
        this.baseStats=baseStats;
    }

    @Override public DataValueDescriptor minValue(){
        return (baseStats.min() == Long.MIN_VALUE || baseStats.min() == Long.MAX_VALUE)
                ?null:wrap(baseStats.min());

    }
    @Override public DataValueDescriptor maxValue(){
        return (baseStats.max() == Long.MIN_VALUE || baseStats.max() == Long.MAX_VALUE)
                ?null:wrap(baseStats.max());
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        LongColumnStatistics.encoder().encode(baseStats,out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.baseStats = this.baseStats = LongColumnStatistics.encoder().decode(in);
    }

    protected abstract DataValueDescriptor wrap(long value);

    protected abstract class TimeDistribution implements Distribution<DataValueDescriptor>{
        private final LongDistribution baseDist;
        private final LongColumnStatistics baseColStats;

        public TimeDistribution(LongColumnStatistics baseColStats){
            this.baseColStats=baseColStats;
            this.baseDist = (LongDistribution)baseColStats.getDistribution();
        }

        @Override
        public long selectivity(DataValueDescriptor dvd){
            if(dvd==null||dvd.isNull())
                return baseColStats.nullCount();
            else{
                long time = unwrapTime(dvd);
                return baseDist.selectivity(time);
            }
        }

        @Override
        public long rangeSelectivity(DataValueDescriptor start,DataValueDescriptor stop,
                                     boolean includeStart,boolean includeStop){
            if(start==null||start.isNull()){
                if(stop==null||stop.isNull()) return baseColStats.nonNullCount();
                return baseDist.selectivityBefore(unwrapTime(stop),includeStop);
            }else{
                long startTime = unwrapTime(start);
                if(stop==null||stop.isNull()) return baseDist.selectivityAfter(startTime,includeStart);
                long stopTime = unwrapTime(stop);
                return baseDist.rangeSelectivity(startTime,stopTime,includeStart,includeStop);
            }
        }

        @Override
        public DataValueDescriptor minValue(){
            return wrap(baseDist.min());
        }

        @Override
        public long minCount(){
            return baseDist.minCount();
        }

        @Override
        public DataValueDescriptor maxValue(){
            return wrap(baseDist.max());
        }

        @Override
        public long totalCount(){
            return baseStats.nonNullCount();
        }

        protected abstract long unwrapTime(DataValueDescriptor dvd);
    }

    protected static abstract class TimeFreq implements FrequencyEstimate<DataValueDescriptor>{
        private LongFrequencyEstimate lfe;

        public TimeFreq(LongFrequencyEstimate lfe){ this.lfe=lfe; }

        @Override public DataValueDescriptor getValue(){ return wrap(lfe.value()); }
        @Override public long count(){ return lfe.count(); }
        @Override public long error(){ return lfe.error(); }

        @Override
        public FrequencyEstimate<DataValueDescriptor> merge(FrequencyEstimate<DataValueDescriptor> other){
            if(other==null) return this;

            assert other instanceof TimeFreq: "Cannot merge frequency estimate of type "+ other.getClass();

            lfe = (LongFrequencyEstimate)lfe.merge(((TimeFreq)other).lfe);
            return this;
        }

        protected abstract DataValueDescriptor wrap(long value);

        @Override
        public String toString(){
            return "("+wrap(lfe.value())+","+lfe.count()+","+lfe.error()+")";
        }
    }

    protected static abstract class TimeFrequentElems implements FrequentElements<DataValueDescriptor>{
        private LongFrequentElements lfe;
        private Function<FrequencyEstimate<? extends Long>,FrequencyEstimate<DataValueDescriptor>> transform;

        public TimeFrequentElems(LongFrequentElements lfe,
                                 Function<FrequencyEstimate<? extends Long>, FrequencyEstimate<DataValueDescriptor>> transform){
            this.lfe=lfe;
            this.transform=transform;
        }

        @Override
        public FrequentElements<DataValueDescriptor> merge(FrequentElements<DataValueDescriptor> fe){
            if(fe==null) return this; //nothing to do
            assert this.getClass()==fe.getClass(): "cannot merge Frequent elements of type "+ fe.getClass();

            lfe = lfe.merge(((TimeFrequentElems)fe).lfe);
            return this;
        }

        @Override
        public FrequentElements<DataValueDescriptor> getClone(){
            return getCopy(lfe.getClone());
        }

        @Override public long totalFrequentElements(){ return lfe.totalFrequentElements(); }

        @Override
        public Set<? extends FrequencyEstimate<DataValueDescriptor>> allFrequentElements(){
            final Set<? extends FrequencyEstimate<Long>> frequencyEstimates=lfe.allFrequentElements();
            return transform(frequencyEstimates);
        }


        @Override
        public Set<? extends FrequencyEstimate<DataValueDescriptor>> frequentElementsBetween(
                DataValueDescriptor start,DataValueDescriptor stop,
                boolean includeStart,boolean includeStop){
            Set<? extends FrequencyEstimate<Long>> dataSet;
            if(start==null||start.isNull()){
                if(stop==null||stop.isNull()) dataSet = lfe.allFrequentElements();
                else{
                    long stopTime = unwrapLong(stop);
                    dataSet = lfe.frequentBefore(stopTime,includeStop);
                }
            }else{
                long startTime = unwrapLong(start);
                if(stop==null||stop.isNull()){
                    dataSet = lfe.frequentAfter(startTime,includeStart);
                }else{
                    long stopTime = unwrapLong(stop);
                    dataSet = lfe.frequentBetween(startTime,stopTime,includeStart,includeStop);
                }
            }
            return transform(dataSet);
        }


        @Override
        public FrequencyEstimate<? extends DataValueDescriptor> equal(DataValueDescriptor dvd){
            assert dvd!=null && !dvd.isNull(): "Cannot estimate equality of null dvds!";
            FrequencyEstimate<? extends Long> longEqual=lfe.equal(unwrapLong(dvd));
            return transform.apply(longEqual);
        }

        protected Set<? extends FrequencyEstimate<DataValueDescriptor>> transform(
                final Set<? extends FrequencyEstimate<Long>> frequencyEstimates){
            return new AbstractSet<FrequencyEstimate<DataValueDescriptor>>(){
                @Override
                public Iterator<FrequencyEstimate<DataValueDescriptor>> iterator(){
                    return Iterators.transform(frequencyEstimates.iterator(),transform);
                }

                @Override public int size(){ return frequencyEstimates.size(); }
            };
        }

        protected abstract FrequentElements<DataValueDescriptor> getCopy(FrequentElements<Long> clone);

        protected abstract long unwrapLong(DataValueDescriptor dvd);
    }
}
