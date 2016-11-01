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
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.yahoo.sketches.quantiles.ItemsSketch;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 * Statistics Container for a key...
 *
 */
public class PrimaryKeyStatisticsImpl implements ItemStatistics<ExecRow> {
    private ItemsSketch<ExecRow> quantilesSketch;
    private long nullCount;
    private ExecRow execRow;

    public PrimaryKeyStatisticsImpl(ExecRow execRow) throws StandardException {
        this(execRow,com.yahoo.sketches.quantiles.ItemsSketch.getInstance(execRow),0l);
    }

    public PrimaryKeyStatisticsImpl(ExecRow execRow, ItemsSketch quantilesSketch,
                                    long nullCount
                                         ) throws StandardException {
        this.quantilesSketch = quantilesSketch;
        this.nullCount = nullCount;
        this.execRow = execRow;
    }

    @Override
    public ExecRow minValue() {
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
        return quantilesSketch.getN();
    }

    @Override
    public ExecRow maxValue() {
        return quantilesSketch.getMaxValue();
    }

    @Override
    public long totalCount() {
        return quantilesSketch.getN()+nullCount;
    }

    @Override
    public long selectivity(ExecRow element) {
        return 1l;
    }

    @Override
    public long rangeSelectivity(ExecRow start, ExecRow stop, boolean includeStart, boolean includeStop) {
        double startSelectivity = start==null?0.0d:quantilesSketch.getCDF(new ExecRow[]{start})[0];
        double stopSelectivity = stop==null?1.0d:quantilesSketch.getCDF(new ExecRow[]{stop})[0];
        return (long) ((stopSelectivity-startSelectivity)*quantilesSketch.getN());
    }
    /*
    @Override
    public ColumnStatisticsMerge<ExecRow> mergeInto(ColumnStatisticsMerge<ExecRow> itemStatisticsBuilder) throws StandardException {
        itemStatisticsBuilder.addQuantilesSketch(quantilesSketch,execRow);
        return itemStatisticsBuilder;
    }
    */

    @Override
    public void update(ExecRow execRow) {
        quantilesSketch.update(execRow);
    }

    @Override
    public String toString() {
        return String.format("PrimaryKeyStatistics{quantiles=%s}",nullCount,quantilesSketch.toString(true,false));
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }
    @Override
    public ItemStatistics<ExecRow> getClone() {
        throw new UnsupportedOperationException("getClone");
    }

    @Override
    public Type getType() {
        return Type.PRIMARYKEY;
    }

}