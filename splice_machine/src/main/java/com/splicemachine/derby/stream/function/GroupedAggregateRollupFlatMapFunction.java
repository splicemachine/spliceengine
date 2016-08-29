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

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.GroupedAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Arrays;
import java.util.Iterator;

/**
 *
 * TODO REVISIT (NOT SCALABLE)
 *
 */
@NotThreadSafe
public class GroupedAggregateRollupFlatMapFunction<Op extends SpliceOperation> extends SpliceFlatMapFunction<Op,LocatedRow,LocatedRow> {
    protected boolean initialized;
    protected GroupedAggregateOperation op;
    protected int[] groupColumns;

    public GroupedAggregateRollupFlatMapFunction() {
        super();
    }

    public GroupedAggregateRollupFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public Iterator<LocatedRow> call(LocatedRow from) throws Exception {
        if (!initialized) {
            initialized = true;
            op = (GroupedAggregateOperation) getOperation();
            groupColumns = op.groupedAggregateContext.getGroupingKeys();
        }
        LocatedRow[] rollupRows = new LocatedRow[groupColumns.length + 1];
        int rollUpPos = groupColumns.length;
        int pos = 0;
        ExecRow nextRow = from.getRow().getClone();
        do {
            rollupRows[pos] = new LocatedRow(nextRow);
            if (rollUpPos > 0) {
                nextRow = nextRow.getClone();
                DataValueDescriptor rollUpCol = nextRow.getColumn(groupColumns[rollUpPos - 1] + 1);
                rollUpCol.setToNull();
            }
            rollUpPos--;
            pos++;
        } while (rollUpPos >= 0);
        return Arrays.asList(rollupRows).iterator();
    }
}