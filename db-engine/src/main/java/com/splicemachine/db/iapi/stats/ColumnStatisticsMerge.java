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
