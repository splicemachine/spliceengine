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
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.GenericAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.stream.iapi.OperationContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Iterator;

/**
 *
 * Mixed record representation...
 *
 * (1) Remove Need for Join on the back end
 * (2) Do not traverse the base data twice.
 * (3) execution vs. planning strategy?
 *
 * emit N records corresponding to number of distincts + non distinct aggregates
 * [distinct1 aggregate columns (value, compute)],[distinct position 0],[distinct value],[group by columns]
 * [distinct2 aggregate columns (value, compute)],[distinct position 1],[distinct value],[group by columns]
 * [non distinct aggregate columns],[null],[null],[group by columns]
 *
 * keyBy distinctPosition,group by columns
 * flatMap --> apply distincts and non distincts based on key
 * key by group by columns
 * merge back together
 *
 */
public class DistinctAggregatesPrepareFunction<Op extends SpliceOperation> extends SpliceFlatMapFunction<Op, ExecRow, ExecRow> {
    private static final long serialVersionUID = 7780564699906451370L;
    private boolean initialized = false;
    protected SpliceGenericAggregator[] aggregates;
    private int[] groupingKeys;
    private int numOfDistinctAggregates;
    private int distinctRowLength;
    private int nonDistinctRowLength;
    private boolean hasNonDistinctAggregate;

    public DistinctAggregatesPrepareFunction() {
    }

    public DistinctAggregatesPrepareFunction(OperationContext<Op> operationContext, int[] groupByColumns) {
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
    public Iterator<ExecRow> call(ExecRow sourceRow) throws Exception {
        if (!initialized) {
            setup();
            initialized = true;
        }
        ArrayList<ExecRow> list = new ArrayList<>(hasNonDistinctAggregate ? numOfDistinctAggregates+1 : numOfDistinctAggregates);
        ValueRow valueRow;

        for (SpliceGenericAggregator aggregator : aggregates) {
            if (!aggregator.isDistinct())
                continue;
            valueRow = new ValueRow(distinctRowLength);
            // copy grouping key values
            // note, column position is 1-based, while the columnid in groupingKeys and
            // uniqueColumns are 0-based
            int j=1;
            for (; groupingKeys != null && j<=groupingKeys.length; j++) {
                valueRow.setColumn(j, sourceRow.getColumn(groupingKeys[j-1]+1));
            }
            // add distinct column id
            valueRow.setColumn(j++, new SQLInteger(aggregator.getInputColumnId()));
            // add aggr result field
            valueRow.setColumn(j++, aggregator.getResultColumnValue(sourceRow));
            // add aggr input filed
            valueRow.setColumn(j++, aggregator.getInputColumnValue(sourceRow));
            // add aggr function field
            valueRow.setColumn(j++, sourceRow.getColumn(aggregator.getAggregatorColumnId()));
            list.add(valueRow);
        }
        // add the row for the non-distinct aggregates
        if (hasNonDistinctAggregate) {
            valueRow = new ValueRow(nonDistinctRowLength);
            //copy grouping key values
            int j = 1;
            for (; groupingKeys != null && j <= groupingKeys.length; j++) {
                valueRow.setColumn(j, sourceRow.getColumn(groupingKeys[j - 1] + 1));
            }
            // add distinct column id
            valueRow.setColumn(j++, new SQLInteger());
            for (SpliceGenericAggregator aggregator : aggregates) {
                if (aggregator.isDistinct())
                    continue;
                valueRow.setColumn(j++, aggregator.getResultColumnValue(sourceRow));
                valueRow.setColumn(j++, aggregator.getInputColumnValue(sourceRow));
                // aggregator columnId is 1-based
                valueRow.setColumn(j++, sourceRow.getColumn(aggregator.getAggregatorColumnId()));
            }
            list.add(valueRow);
        }

        return list.iterator();
    }

    private void setup() {
        GenericAggregateOperation op = (GenericAggregateOperation)operationContext.getOperation();
        aggregates = op.aggregates;
        numOfDistinctAggregates = 0;
        for (SpliceGenericAggregator aggregator : aggregates) {
            if (aggregator.isDistinct())
                numOfDistinctAggregates++;
        }
        int numOfGroupKeys = 0;
        if (groupingKeys != null)
            numOfGroupKeys = groupingKeys.length;
        distinctRowLength = numOfGroupKeys + 4;
        nonDistinctRowLength = numOfGroupKeys + 1
                + (aggregates.length - numOfDistinctAggregates)*3;
        hasNonDistinctAggregate = (aggregates.length != numOfDistinctAggregates);

        return;
    }

}
