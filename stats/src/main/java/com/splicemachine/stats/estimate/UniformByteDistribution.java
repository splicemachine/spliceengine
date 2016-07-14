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

package com.splicemachine.stats.estimate;

import com.splicemachine.stats.ByteColumnStatistics;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.frequency.ByteFrequencyEstimate;
import com.splicemachine.stats.frequency.ByteFrequentElements;
import com.splicemachine.utils.ComparableComparator;

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 3/5/15
 */
public class UniformByteDistribution extends UniformDistribution<Byte> implements ByteDistribution{
    public UniformByteDistribution(ColumnStatistics<Byte> columnStats) {
        super(columnStats, ComparableComparator.<Byte>newComparator());
    }


    @Override
    public long selectivity(byte value) {
        ByteColumnStatistics bcs = (ByteColumnStatistics)columnStats;
        if(value==bcs.min())
            return bcs.minCount();

        ByteFrequentElements bfe = (ByteFrequentElements)bcs.topK();
        ByteFrequencyEstimate byteFrequencyEstimate = bfe.countEqual(value);
        if(byteFrequencyEstimate.count()>0) return byteFrequencyEstimate.count();

        return uniformEstimate();
    }

    @Override
    public long selectivityBefore(byte stop, boolean includeStop) {
        ByteColumnStatistics bcs = (ByteColumnStatistics)columnStats;
        byte min = bcs.min();
        if(stop<min ||(!includeStop && stop==min)) return 0l;

        return rangeSelectivity(min,stop,true,includeStop);
    }

    @Override
    public long selectivityAfter(byte start, boolean includeStart) {
        ByteColumnStatistics bcs = (ByteColumnStatistics)columnStats;
        byte max = bcs.max();
        if(start>max||(!includeStart && start==max)) return 0l;

        return rangeSelectivity(start,max,includeStart,true);
    }

    @Override
    protected long estimateEquals(Byte element) {
        return selectivity(element.byteValue());
    }

    @Override
    public long rangeSelectivity(byte start, byte stop, boolean includeStart, boolean includeStop) {
        ByteColumnStatistics bcs = (ByteColumnStatistics)columnStats;
        byte min = bcs.min();
        byte max = bcs.max();

        if(stop< min ||(!includeStop && stop== min)) return 0l;
        else if(includeStop && stop== min)
            return selectivity(stop);
        if(start> max ||(!includeStart && start== max)) return 0l;
        else if(includeStart && start == max)
            return selectivity(start);

        /*
         * Now adjust the range to deal with running off the end points of the range
         */
        boolean isMin = false;
        if(start<=min) {
            start = min;
            isMin = true;
        }
        if(stop> max)
            stop = max;

        return rangeSelectivity(start,stop,includeStart,includeStop,isMin);
    }

    public long cardinalityBefore(byte stop,boolean includeStop){
        ByteColumnStatistics bcs = (ByteColumnStatistics)columnStats;
        byte min = bcs.min();
        if(stop<min ||(!includeStop && stop==min)) return 0l;

        return rangeCardinality(min,stop,true,includeStop);
    }

    public long cardinalityAfter(byte start,boolean includeStart){
        ByteColumnStatistics bcs = (ByteColumnStatistics)columnStats;
        byte max = bcs.max();
        if(start>max||(!includeStart && start==max)) return 0l;

        return rangeCardinality(start,max,includeStart,true);
    }

    public long rangeCardinality(byte start,byte stop,boolean includeStart,boolean includeStop){
        ByteColumnStatistics bcs = (ByteColumnStatistics)columnStats;
        byte min = bcs.min();
        byte max = bcs.max();

        if(stop< min ||(!includeStop && stop== min)) return 0l;
        else if(includeStop && stop== min)
            return selectivity(stop);
        if(start> max ||(!includeStart && start== max)) return 0l;
        else if(includeStart && start == max)
            return selectivity(start);

        /*
         * Now adjust the range to deal with running off the end points of the range
         */
        if(start<=min) {
            start = min;
        }
        if(stop> max)
            stop = max;

        int c = stop-start;
        if(!includeStart) c--;
        if(includeStop) c++;
        return c;
    }

    @Override public byte min(){ return ((ByteColumnStatistics)columnStats).min(); }
    @Override public byte max(){ return ((ByteColumnStatistics)columnStats).max(); }
    @Override public Byte minValue(){ return min(); }
    @Override public long minCount(){ return columnStats.minCount(); }
    @Override public Byte maxValue(){ return max(); }
    @Override public long totalCount(){ return columnStats.nonNullCount(); }

    public long cardinality(){ return columnStats.cardinality(); }

    public long cardinality(Byte start,Byte stop,boolean includeStart,boolean includeStop){
        if(start==null){
            if(stop==null) return cardinality();
            else return cardinalityBefore(stop.byteValue(),includeStop);
        }else if(stop==null) return cardinalityAfter(start.byteValue(),includeStart);
        else return rangeCardinality(start,stop,includeStart,includeStop);
    }

    @Override
    protected long estimateRange(Byte start, Byte stop, boolean includeStart, boolean includeStop, boolean isMin) {
        return rangeSelectivity(start,stop,includeStart,includeStop,isMin);
    }

    @Override
    protected long getAdjustedRowCount() {
        long rowCount = columnStats.nonNullCount();
        ByteFrequentElements frequentElements = (ByteFrequentElements)columnStats.topK();
        rowCount-=frequentElements.totalFrequentElements();
        if(frequentElements.equal(((ByteColumnStatistics)columnStats).min()).count()<=0)
            rowCount-=columnStats.minCount();
        return rowCount;
    }

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private long rangeSelectivity(byte start, byte stop, boolean includeStart, boolean includeStop, boolean isMin) {
        long perRowCount = uniformEstimate();
        long baseEst = perRowCount*(stop-start);
        /*
         * Now adjust using Frequent Elements
         */
        ByteFrequentElements bfe = (ByteFrequentElements)columnStats.topK();
        //if we are the min value, don't include the start key in frequent elements
        includeStart = includeStart &&!isMin;
        Set<ByteFrequencyEstimate> frequencyEstimates = bfe.frequentBetween(start, stop, includeStart, includeStop);
        return uniformRangeCount(includeStart,includeStop,baseEst,frequencyEstimates);
    }

}
