/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */
package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.GenericAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.stream.iapi.OperationContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by yxia on 5/28/17.
 */
public class StitchMixedRowFunction<Op extends SpliceOperation> extends SpliceFunction2<Op, ExecRow, ExecRow, ExecRow> implements Serializable {
    protected GenericAggregateOperation aggregateOperation;
    protected int[] groupingKeys;
    protected int numOfGroupKeys;
    protected SpliceGenericAggregator[] aggregates;

    protected boolean initialized;
    protected HashMap<Integer, SpliceGenericAggregator> distinctAggregateMap;

    public StitchMixedRowFunction() {
    }

    public StitchMixedRowFunction(OperationContext<Op> operationContext,
                                  int[] groupByColumns) {
        super(operationContext);
        groupingKeys = groupByColumns;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        ArrayUtil.writeIntArray(out, groupingKeys);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        groupingKeys = ArrayUtil.readIntArray(in);
    }

    @Override
    public ExecRow call(ExecRow srcRow1, ExecRow srcRow2) throws Exception {
        if (!initialized) {
            aggregateOperation = (GenericAggregateOperation)operationContext.getOperation();
            aggregates = aggregateOperation.aggregates;
            numOfGroupKeys = groupingKeys==null? 0: groupingKeys.length;

            // build a map between distinct column to aggregator
            distinctAggregateMap = new HashMap<>();
            for (SpliceGenericAggregator aggr:aggregates) {
                if (!aggr.isDistinct())
                    continue;
                distinctAggregateMap.put(new Integer(aggr.getInputColumnId()), aggr);
            }
            initialized = true;
        }
        operationContext.recordRead();
        ExecRow valueRow;

        if (srcRow1 == null) {
            valueRow = aggregateOperation.getExecRowDefinition();
            copyRow(valueRow, srcRow2);
            return valueRow;
        }
        if (srcRow2 == null) {
            valueRow = aggregateOperation.getExecRowDefinition();
            copyRow(valueRow, srcRow1);
            return valueRow;
        }

        int outputRowSize = aggregateOperation.getExecRowDefinition().size();

        if (srcRow1.size() == outputRowSize) {
            valueRow = srcRow1;
            if (srcRow1.size() == srcRow2.size())
                mergeRow(valueRow, srcRow2);
            else
                copyRow(valueRow, srcRow2);
        } else if (srcRow2.size() == outputRowSize) {
            valueRow = srcRow2;
            copyRow(valueRow, srcRow1);
        } else {
            // neither row is the accumulated row,
            // compose a row with all the aggregates together
            valueRow = aggregateOperation.getExecRowDefinition();
            copyRow(valueRow, srcRow1);
            copyRow(valueRow, srcRow2);
        }

        return valueRow;
    }

    private void copyRow(ExecRow valueRow, ExecRow src) throws Exception {
        int j=1;
        for (; groupingKeys != null && j<=groupingKeys.length; j++) {
            valueRow.setColumn(groupingKeys[j-1]+1, src.getColumn(j));
        }

        SpliceGenericAggregator aggregator = distinctAggregateMap.get(src.getColumn(j++).getInt());
        if (aggregator != null) {
            valueRow.setColumn(aggregator.getResultColumnId(), src.getColumn(j++));
            valueRow.setColumn(aggregator.getInputColumnId(), src.getColumn(j++));
            valueRow.setColumn(aggregator.getAggregatorColumnId(), src.getColumn(j++));
        } else { // non-distinct aggregates
            for (SpliceGenericAggregator aggr: aggregates) {
                if (!aggr.isDistinct()) {
                    valueRow.setColumn(aggr.getResultColumnId(), src.getColumn(j++));
                    valueRow.setColumn(aggr.getInputColumnId(), src.getColumn(j++));
                    valueRow.setColumn(aggr.getAggregatorColumnId(), src.getColumn(j++));
                }
            }
        }
        return;
    }

    private void mergeRow(ExecRow valueRow, ExecRow src) throws Exception {
        for (int j=numOfGroupKeys+1; j<=numOfGroupKeys + aggregates.length*3; j++) {
            DataValueDescriptor dvd = src.getColumn(j);
            if (!dvd.isNull())
                valueRow.setColumn(j, dvd);
        }
        return;
    }
}
