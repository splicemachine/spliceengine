/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.db.iapi.stats;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.UpdateSketch;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 * Statistics Container for a column wrapping quantiles, frequencies and theta sketches provided
 * via the Yahoo Sketches open source implementation.
 *
 * @see <a href="https://datasketches.github.io/">https://datasketches.github.io/</a>
 */
public class ColumnStatisticsImpl implements ItemStatistics<DataValueDescriptor>, Externalizable {
    protected com.yahoo.sketches.quantiles.ItemsSketch<DataValueDescriptor> quantilesSketch;
    protected com.yahoo.sketches.frequencies.ItemsSketch<DataValueDescriptor> frequenciesSketch;
    protected Sketch thetaSketch;
    protected long nullCount;
    protected DataValueDescriptor dvd;

    public ColumnStatisticsImpl() {

    }

    /**
     *
     * Generates the stats implementation based on defaults for the data type (DataValueDescriptor)
     *
     * @param dvd
     * @throws StandardException
     */
    public ColumnStatisticsImpl(DataValueDescriptor dvd) throws StandardException {
        this(dvd, dvd.getQuantilesSketch(),dvd.getFrequenciesSketch(),dvd.getThetaSketch(),0l);
    }

    /**
     *
     *
     *
     * @param dvd
     * @param quantilesSketch
     * @param frequenciesSketch
     * @param thetaSketch
     * @param nullCount
     */
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
        byte[] quantilesSketchBytes = quantilesSketch.toByteArray(new DVDArrayOfItemsSerDe(dvd));
        out.writeInt(quantilesSketchBytes.length);
        out.write(quantilesSketchBytes);
        byte[] frequenciesSketchBytes = frequenciesSketch.toByteArray(new DVDArrayOfItemsSerDe(dvd));
        out.writeInt(frequenciesSketchBytes.length);
        out.write(frequenciesSketchBytes);
        byte[] thetaSketchBytes = thetaSketch.toByteArray();
        out.writeInt(thetaSketchBytes.length);
        out.write(thetaSketchBytes);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        NativeMemory quantMem = null;
        NativeMemory freqMem = null;
        NativeMemory thetaMem = null;
        try {
            nullCount = in.readLong();
            dvd = (DataValueDescriptor) in.readObject();
            byte[] quantiles = new byte[in.readInt()];
            in.readFully(quantiles);
            quantMem = new NativeMemory(quantiles);
            quantilesSketch = com.yahoo.sketches.quantiles.ItemsSketch.getInstance(quantMem, dvd, new DVDArrayOfItemsSerDe(dvd));
            byte[] frequencies = new byte[in.readInt()];
            in.readFully(frequencies);
            freqMem = new NativeMemory(frequencies);
            frequenciesSketch = com.yahoo.sketches.frequencies.ItemsSketch.getInstance(freqMem, new DVDArrayOfItemsSerDe(dvd));
            byte[] thetaSketchBytes = new byte[in.readInt()];
            in.readFully(thetaSketchBytes);
            thetaMem = new NativeMemory(thetaSketchBytes);
            thetaSketch = Sketch.heapify(thetaMem);
        } finally {
            if (quantMem!=null)
                quantMem.freeMemory();
            if (freqMem!=null)
                freqMem.freeMemory();
            if (thetaMem!=null)
                thetaMem.freeMemory();

        }
    }

    /**
     *
     * Retrieves the minimum value from the quantile sketch
     *
     * @return
     */
    @Override
    public DataValueDescriptor minValue() {
        return quantilesSketch.getMinValue();
    }
    /**
     *
     * Retrieves the null count that is recorded during computation.
     *
     * @return
     */
    @Override
    public long nullCount() {
        return nullCount;
    }
    /**
     *
     * Retrieves the not null count from the quantiles sketch.
     *
     * @return
     */
    @Override
    public long notNullCount() {
        return quantilesSketch.getN();
    }
    /**
     *
     * Retrieves the number of unique records via the ThetaSketch.
     *
     * @return
     */
    @Override
    public long cardinality() {
        return (long)thetaSketch.getEstimate();
    }

    /**
     *
     * Retrieves the maximum value via the quantiles sketch.
     *
     * @return
     */
    @Override
    public DataValueDescriptor maxValue() {
        return quantilesSketch.getMaxValue();
    }
    /**
     *
     * Retrieves the total count as a summation of the not null count and the null count.
     *
     * @return
     */

    @Override
    public long totalCount() {
        return quantilesSketch.getN()+nullCount;
    }

    /**
     *
     * Number of Records Selected from this columns implementation.
     *
     * If
     *  element is null then nullCount
     * elseif
     *  frequent element then frequencies sketch estimate
     * else {
     *     quantilesSketch total count/thetaSketch estimate
     * }
     *
     * @param element the element to match
     * @return
     */
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
        return (long) ( ((double) quantilesSketch.getN())/thetaSketch.getEstimate()); // Should we remove frequent items?
    }

    /**
     *
     * Using the Cumulative Distribution Function from the quantiles sketch to determine the
     * number of records that are in the range.
     *
     * @param start the start of the range to estimate. If {@code null}, then scan everything before {@code stop}.
     *              If {@code stop} is also {@code null}, then this will return an estimate to the number of entries
     *              in the entire data set.
     * @param stop the end of the range to estimate. If {@code null}, then scan everything after {@code start}.
     *             If {@code start} is also {@code null}, then this will return an estimate of the number of entries
     *             in the entire data set.
     * @param includeStart if {@code true}, then include entries which are equal to {@code start}
     * @param includeStop if {@code true}, then include entries which are <em>equal</em> to {@code stop}
     * @return
     */
    @Override
    public long rangeSelectivity(DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop) {
        if (!includeStart && start!=null&& !start.isNull())
            start = new StatsExcludeStartDVD(start);
        double startSelectivity = start==null||start.isNull()?0.0d:quantilesSketch.getCDF(new DataValueDescriptor[]{start})[0];
        if (includeStop && stop !=null && !stop.isNull())
            stop = new StatsIncludeEndDVD(stop);
        double stopSelectivity = stop==null||stop.isNull()?1.0d:quantilesSketch.getCDF(new DataValueDescriptor[]{stop})[0];
        double totalSelectivity = stopSelectivity-startSelectivity;
        double count = (double)quantilesSketch.getN();
        if (totalSelectivity==Double.NaN || count == 0)
            return 0;
        return Math.round(totalSelectivity * count);
    }

    /**
     *
     * Updating the column's value for the three sketches and the null count.
     *
     * @param dvd
     */
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

    /**
     *
     * Shallow copy of the statistics.
     *
     * @return
     */
    @Override
    public ItemStatistics<DataValueDescriptor> getClone() {
        return new ColumnStatisticsImpl(dvd.cloneValue(false),
                quantilesSketch,
                frequenciesSketch,
                thetaSketch,
                nullCount);
    }

    /**
     *
     * COLUMN
     *
     * @return
     */
    @Override
    public Type getType() {
        return Type.COLUMN;
    }

    /**
     *
     * Column DataValueDescriptor (type information)
     *
     * @return
     */
    public DataValueDescriptor getColumnDescriptor() {
        return dvd;
    }

    /**
     *
     * Retrieve the quantiles sketch directly.
     *
     * @return
     */
    public ItemsSketch<DataValueDescriptor> getQuantilesSketch() {
        return quantilesSketch;
    }
    /**
     *
     * Retrieve the frequencies sketch directly.
     *
     * @return
     */
    public com.yahoo.sketches.frequencies.ItemsSketch<DataValueDescriptor> getFrequenciesSketch() {
        return frequenciesSketch;
    }
    /**
     *
     * Retrieve the theta sketch directly.
     *
     * @return
     */
    public Sketch getThetaSketch() {
        return thetaSketch;
    }

}