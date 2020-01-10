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

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.GenericAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.commons.collections.iterators.SingletonIterator;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by yxia on 6/27/17.
 */
public class StitchMixedRowFlatMapFunction<Op extends SpliceOperation> extends SpliceFlatMapFunction<Op, Iterator<ExecRow>, ExecRow> {
    protected boolean initialized;
    protected GenericAggregateOperation op;
    protected SpliceGenericAggregator[] aggregates;
    protected HashMap<Integer, SpliceGenericAggregator> distinctAggregateMap;

    public StitchMixedRowFlatMapFunction() {
    }

    public StitchMixedRowFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<ExecRow> call(Iterator<ExecRow> rowIterator) throws Exception {
        if (!initialized) {
            op = (GenericAggregateOperation) getOperation();
            aggregates = op.aggregates;
            // build a map between distinct column to aggregator
            distinctAggregateMap = new HashMap<>();
            for (SpliceGenericAggregator aggr:aggregates) {
                if (!aggr.isDistinct())
                    continue;
                distinctAggregateMap.put(new Integer(aggr.getInputColumnId()), aggr);
            }
            initialized = true;
        }

        ExecRow valueRow = op.getSourceExecIndexRow().getClone();
        if (!rowIterator.hasNext()) {
            op.finishAggregation(valueRow);
            return new SingletonIterator(valueRow);
        }

        while (rowIterator.hasNext()) {
            ExecRow r2 = rowIterator.next();
            copyRow(valueRow, r2);
        }

        for(SpliceGenericAggregator aggregator:aggregates){
            if (!aggregator.isInitialized(valueRow)) {
                aggregator.initializeAndAccumulateIfNeeded(valueRow, valueRow);
            }
            aggregator.finish(valueRow);
        }
        op.setCurrentRow(valueRow);
        return new SingletonIterator(valueRow);
    }

    private void copyRow(ExecRow valueRow, ExecRow src) throws Exception {
        int j=1;
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
