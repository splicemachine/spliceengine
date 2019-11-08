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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.db.iapi.stats;

import com.splicemachine.db.agg.Aggregator;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLBlob;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.yahoo.sketches.quantiles.ItemsUnion;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;

import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

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
    protected long nullCount = 0L;
    protected DataValueDescriptor dvd;
    protected long totalCount = 0L;
    protected long cardinality = 0L;
    protected ItemStatistics.Type columnStatsType = ItemStatistics.Type.COLUMN;


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
        if (!initialized) {
            if (value instanceof FakeColumnStatisticsImpl) {
                columnStatsType = ItemStatistics.Type.FAKE;
                FakeColumnStatisticsImpl fakeColumnStatistics = (FakeColumnStatisticsImpl) value;
                nullCount += fakeColumnStatistics.nullCount();
                totalCount += fakeColumnStatistics.totalCount();
                cardinality = fakeColumnStatistics.cardinality();
                initialized = true;
                return;
            }

            dvd = value.getColumnDescriptor();
            assert dvd!=null:"dvd should not be null";
            thetaSketchUnion = Sketches.setOperationBuilder().buildUnion();
            quantilesSketchUnion = ItemsUnion.getInstance(value.getQuantilesSketch().getK(), value.getColumnDescriptor());
            frequenciesSketch =  dvd.getFrequenciesSketch();
            initialized = true;
        }

        if (!value.getType().equals(columnStatsType))
            throw new RuntimeException("Multiple column stats type co-exists!");

        if (value instanceof FakeColumnStatisticsImpl) {
            FakeColumnStatisticsImpl fakeColumnStatistics = (FakeColumnStatisticsImpl) value;
            nullCount += fakeColumnStatistics.nullCount();
            totalCount += fakeColumnStatistics.totalCount();
            if (cardinality < fakeColumnStatistics.cardinality())
                cardinality = fakeColumnStatistics.cardinality();
        }

        assert value.quantilesSketch !=null && value.frequenciesSketch !=null && value.thetaSketch !=null && value!=null:"Sketches should not be null";
        quantilesSketchUnion.update(value.quantilesSketch);
        frequenciesSketch.merge(value.frequenciesSketch);
        thetaSketchUnion.update(value.thetaSketch);
        nullCount =+ value.nullCount();
    }

    /**
     * Merges two aggregated sets of statistics.
     *
     * @param otherAggregator
     */
    @Override
    public void merge(ColumnStatisticsMerge otherAggregator) {
        if (!columnStatsType.equals(otherAggregator.columnStatsType))
            throw new RuntimeException("Multiple column stats type co-exists!");

        if (columnStatsType == ItemStatistics.Type.FAKE) {
            nullCount += otherAggregator.nullCount;
            totalCount += otherAggregator.totalCount;
            if (cardinality  < otherAggregator.cardinality)
                cardinality = otherAggregator.cardinality;
            return;
        }

        ColumnStatisticsMerge columnStatisticsMerge = otherAggregator;
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
        if (columnStatsType == ItemStatistics.Type.FAKE) {
            return new FakeColumnStatisticsImpl(dvd, nullCount, totalCount, cardinality);
        }

        return new ColumnStatisticsImpl(dvd,quantilesSketchUnion.getResult(),frequenciesSketch,thetaSketchUnion.getResult(),nullCount);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(terminate());
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

    public ExecRow toExecRow(Integer columnId) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(terminate());
            baos.close();
            ExecRow result = new ValueRow(2);
            result.setColumn(1, new SQLInteger(columnId));
            result.setColumn(2, new SQLBlob(baos.toByteArray()));
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
