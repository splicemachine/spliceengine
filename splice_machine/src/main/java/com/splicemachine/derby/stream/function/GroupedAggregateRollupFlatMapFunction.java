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