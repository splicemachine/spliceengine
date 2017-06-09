/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.GroupedAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.GroupedAggregateContext;
import com.splicemachine.derby.stream.iapi.OperationContext;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by yxia on 5/28/17.
 */
public class StitchMixedRowFunction<Op extends SpliceOperation> extends SpliceFunction2<Op, LocatedRow, LocatedRow, LocatedRow> implements Serializable {
    private GroupedAggregateOperation groupedAggregateOperation;
    private GroupedAggregateContext groupedAggregateContext;
    private int[] groupingKeys;
    protected SpliceGenericAggregator[] aggregates;

    protected boolean initialized;
    protected HashMap<Integer, SpliceGenericAggregator> distinctAggregateMap;

    public StitchMixedRowFunction() {
    }

    public StitchMixedRowFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public LocatedRow call(LocatedRow locatedRow1, LocatedRow locatedRow2) throws Exception {
        if (!initialized) {
            groupedAggregateOperation = (GroupedAggregateOperation)operationContext.getOperation();
            groupedAggregateContext = groupedAggregateOperation.groupedAggregateContext;
            groupingKeys = groupedAggregateContext.getGroupingKeys();
            aggregates = groupedAggregateOperation.aggregates;

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
        LocatedRow resultRow;
        if (locatedRow1 == null || locatedRow1.size() < groupingKeys.length + aggregates.length*3) {
            // compose a row with all the aggregates together
            valueRow = new ValueRow(groupingKeys.length + aggregates.length*3);

            if (locatedRow1 != null)
                copyRow(valueRow, locatedRow1.getRow());
            resultRow = new LocatedRow(valueRow);
        } else {
            valueRow = locatedRow1.getRow();
            resultRow = locatedRow1;
        }
        if (locatedRow2 == null) return resultRow;

        copyRow(valueRow, locatedRow2.getRow());
        return resultRow;
    }

    private void copyRow(ExecRow valueRow, ExecRow src) throws Exception {
        int j=1;
        for (; j<=groupingKeys.length; j++) {
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
}