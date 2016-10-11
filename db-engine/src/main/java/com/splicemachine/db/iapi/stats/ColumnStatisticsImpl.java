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
package com.splicemachine.db.iapi.stats;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.yahoo.memory.AllocMemory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.UpdateSketch;
import org.apache.spark.unsafe.memory.MemoryAllocator;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 *
 * Statistics Container for a column.
 *
 */
public class ColumnStatisticsImpl implements ItemStatistics<DataValueDescriptor>, Externalizable {
    protected com.yahoo.sketches.quantiles.ItemsSketch<DataValueDescriptor> quantilesSketch;
    protected com.yahoo.sketches.frequencies.ItemsSketch<DataValueDescriptor> frequenciesSketch;
    protected Sketch thetaSketch;
    protected long nullCount;
    protected DataValueDescriptor dvd;

    public ColumnStatisticsImpl() {

    }
    public ColumnStatisticsImpl(DataValueDescriptor dvd) throws StandardException {
        this(dvd, dvd.getQuantilesSketch(),dvd.getFrequenciesSketch(),dvd.getThetaSketch(),0l);
    }

    public ColumnStatisticsImpl(DataValueDescriptor dvd,
                                com.yahoo.sketches.quantiles.ItemsSketch quantilesSketch,
                                com.yahoo.sketches.frequencies.ItemsSketch frequenciesSketch,
                                Sketch thetaSketch, long nullCount
                                         ) {
        this.dvd = dvd;
        this.quantilesSketch = quantilesSketch;
        this.frequenciesSketch = frequenciesSketch;
        this.thetaSketch = thetaSketch;
        this.nullCount = nullCount;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(nullCount);
        out.writeObject(dvd);
        byte[] quantilesSketchBytes = quantilesSketch.toByteArray(new DVDArrayOfItemsSerDe(dvd,0));
        out.writeInt(quantilesSketchBytes.length);
        out.write(Arrays.copyOf(quantilesSketchBytes,quantilesSketchBytes.length));
        byte[] frequenciesSketchBytes = frequenciesSketch.toByteArray(new DVDArrayOfItemsSerDe(dvd,0));
        out.writeInt(frequenciesSketchBytes.length);
        out.write(frequenciesSketchBytes);
        byte[] thetaSketchBytes = thetaSketch.toByteArray();
        out.writeInt(thetaSketchBytes.length);
        out.write(thetaSketchBytes);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        nullCount = in.readLong();
        dvd = (DataValueDescriptor) in.readObject();
        byte[] quantiles = new byte[in.readInt()];
        in.readFully(quantiles);
        quantilesSketch = com.yahoo.sketches.quantiles.ItemsSketch.getInstance(new NativeMemory(quantiles), dvd ,new DVDArrayOfItemsSerDe(dvd,quantiles.length));
        byte[] frequencies = new byte[in.readInt()];
        in.readFully(frequencies);
        frequenciesSketch = com.yahoo.sketches.frequencies.ItemsSketch.getInstance(new NativeMemory(frequencies), new DVDArrayOfItemsSerDe(dvd, frequencies.length));
        byte[] thetaSketchBytes = new byte[in.readInt()];
        in.readFully(thetaSketchBytes);
        thetaSketch = Sketch.heapify(new NativeMemory(thetaSketchBytes));
    }

    @Override
    public DataValueDescriptor minValue() {
        return quantilesSketch.getMinValue();
    }

    @Override
    public long nullCount() {
        return nullCount;
    }

    @Override
    public long notNullCount() {
        return quantilesSketch.getN();
    }

    @Override
    public long cardinality() {
        return (long)thetaSketch.getEstimate();
    }

    @Override
    public DataValueDescriptor maxValue() {
        return quantilesSketch.getMaxValue();
    }

    @Override
    public long totalCount() {
        return quantilesSketch.getN()+nullCount;
    }

    @Override
    public long selectivity(DataValueDescriptor element) {
        // Use Null Data
        if (element == null || element.isNull())
            return nullCount;
        // Frequent Items Sketch?
        long count = frequenciesSketch.getEstimate(element);
        if (count>0)
            return count;
        // Return Cardinality Based Estimate
        return (long) (quantilesSketch.getN()/thetaSketch.getEstimate()); // Should we remove frequent items?
    }

    @Override
    public long rangeSelectivity(DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop) {
        double startSelectivity = start==null||start.isNull()?0.0d:quantilesSketch.getCDF(new DataValueDescriptor[]{start})[0];
        double stopSelectivity = stop==null||stop.isNull()?1.0d:quantilesSketch.getCDF(new DataValueDescriptor[]{stop})[0];
        return (long) ((stopSelectivity-startSelectivity)*quantilesSketch.getN());
    }
    @Override
    public void update(DataValueDescriptor dvd) {
        if (dvd.isNull()) {
            nullCount++;
        } else {
            frequenciesSketch.update(dvd);
            quantilesSketch.update(dvd);
            dvd.updateThetaSketch((UpdateSketch) thetaSketch);
        }
    }

    @Override
    public String toString() {
        return String.format("Statistics{nullCount=%d, frequencies=%s, quantiles=%s, theta=%s}",nullCount,frequenciesSketch,quantilesSketch.toString(true,false),thetaSketch.toString());
    }

    @Override
    public ItemStatisticsBuilder<DataValueDescriptor> mergeInto(ItemStatisticsBuilder<DataValueDescriptor> itemStatisticsBuilder) throws StandardException {
        itemStatisticsBuilder.addFrequenciesSketch(frequenciesSketch)
        .addThetaSketch(thetaSketch)
        .addQuantilesSketch(quantilesSketch,dvd)
        .addNullCount(nullCount);
        return itemStatisticsBuilder;
    }
}