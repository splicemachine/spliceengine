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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
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
        this(execRow,com.yahoo.sketches.quantiles.ItemsSketch.getInstance(execRow), 0L);
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
        return 1L;
    }

    @Override
    public long rangeSelectivity(ExecRow start, ExecRow stop, boolean includeStart, boolean includeStop, boolean useExtrapolation) {
        double startSelectivity = start==null?0.0d:quantilesSketch.getCDF(new ExecRow[]{start})[0];
        double stopSelectivity = stop==null?1.0d:quantilesSketch.getCDF(new ExecRow[]{stop})[0];
        return (long) ((stopSelectivity-startSelectivity)*quantilesSketch.getN());
    }

    @Override
    public long rangeSelectivity(ExecRow start, ExecRow stop, boolean includeStart, boolean includeStop) {
        return rangeSelectivity(start, stop, includeStart, includeStop, false);
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
        return String.format("PrimaryKeyStatistics{quantiles=%s}",quantilesSketch.toString(true,false));
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

    @Override
    public long selectivityExcludingValueIfSkewed(ExecRow value) {
        throw new UnsupportedOperationException("selectivityExcludingValueIfSkewed");
    }
}
