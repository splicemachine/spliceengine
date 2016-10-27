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

import com.splicemachine.db.agg.Aggregator;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.yahoo.sketches.quantiles.ItemsUnion;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 * Aggregator for merging together column statistics.
 *
 * @see Aggregator
 *
 */
public class ColumnStatisticsMerge implements Aggregator<ColumnStatisticsImpl, ColumnStatisticsImpl, ColumnStatisticsMerge>, Externalizable {
    protected boolean initialized;
    protected Union thetaSketchUnion;
    protected ItemsUnion quantilesSketchUnion;
    protected com.yahoo.sketches.frequencies.ItemsSketch<DataValueDescriptor> frequenciesSketch;
    protected long nullCount = 0l;
    protected DataValueDescriptor dvd;

    public ColumnStatisticsMerge() {

    }

    public static ColumnStatisticsMerge instance() {
        return new ColumnStatisticsMerge();
    }
    /*
     * No-op init
     *
     */
    @Override
    public void init() {

    }

    /**
     *
     * Accumulate path, check for initialization and then merge sketches.
     *
     * @param value
     * @throws StandardException
     */
    @Override
    public void accumulate(ColumnStatisticsImpl value) throws StandardException {
        ColumnStatisticsImpl columnStatistics = (ColumnStatisticsImpl) value;
        if (!initialized) {
            dvd = columnStatistics.getColumnDescriptor();
            assert dvd!=null:"dvd should not be null";
            thetaSketchUnion = Sketches.setOperationBuilder().buildUnion();
            quantilesSketchUnion = ItemsUnion.getInstance(value.getQuantilesSketch().getK(), ((ColumnStatisticsImpl)value).getColumnDescriptor());
            frequenciesSketch =  dvd.getFrequenciesSketch();
            initialized = true;
        }
        assert columnStatistics.quantilesSketch !=null && columnStatistics.frequenciesSketch !=null && columnStatistics.thetaSketch !=null && value!=null:"Sketches should not be null";
        quantilesSketchUnion.update(columnStatistics.quantilesSketch);
        frequenciesSketch.merge(columnStatistics.frequenciesSketch);
        thetaSketchUnion.update(columnStatistics.thetaSketch);
        nullCount =+ columnStatistics.nullCount();
    }

    /**
     * Merges two aggregated sets of statistics.
     *
     * @param otherAggregator
     */
    @Override
    public void merge(ColumnStatisticsMerge otherAggregator) {
        ColumnStatisticsMerge columnStatisticsMerge = (ColumnStatisticsMerge) otherAggregator;
        quantilesSketchUnion.update(columnStatisticsMerge.quantilesSketchUnion.getResult());
        frequenciesSketch.merge(columnStatisticsMerge.frequenciesSketch);
        thetaSketchUnion.update(columnStatisticsMerge.thetaSketchUnion.getResult());
        nullCount =+ columnStatisticsMerge.nullCount;
    }

    /**
     *
     * Generate the effective column statistics.
     *
     * @return
     */
    @Override
    public ColumnStatisticsImpl terminate() {
        return new ColumnStatisticsImpl(dvd,quantilesSketchUnion.getResult(),frequenciesSketch,thetaSketchUnion.getResult(),nullCount);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        terminate().writeExternal(out);
    }

    /*

    Steven Le Roux

     */

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        try {
            accumulate((ColumnStatisticsImpl) in.readObject());
        } catch (StandardException se) {
            throw new IOException(se);
        }

    }
}
