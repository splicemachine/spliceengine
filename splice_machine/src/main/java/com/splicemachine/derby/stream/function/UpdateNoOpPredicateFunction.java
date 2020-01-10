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
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.UpdateOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import javax.annotation.Nullable;

/**
 *
 * Filters out rows where the rows have not changed.  This keeps a lot of writes from happening when the rows have
 * not changed.
 *
 */
public class UpdateNoOpPredicateFunction<Op extends SpliceOperation> extends SplicePredicateFunction<Op,ExecRow> {
    protected UpdateOperation op;
    protected boolean initialized = false;
    public UpdateNoOpPredicateFunction() {
        super();
    }

    public UpdateNoOpPredicateFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public boolean apply(@Nullable ExecRow locatedRow) {
        if (!initialized) {
            op = (UpdateOperation) operationContext.getOperation();
            initialized = true;
        }
        try {
            DataValueDescriptor[] sourRowValues = locatedRow.getRowArray();
            for (int i = op.getHeapList().anySetBit(), oldPos = 0; i >= 0; i = op.getHeapList().anySetBit(i), oldPos++) {
                DataValueDescriptor oldVal = sourRowValues[oldPos];
                DataValueDescriptor newVal = sourRowValues[op.getColumnPositionMap(op.getHeapList())[i]];
                if (!newVal.equals(oldVal)) {
                    return true; // Changed Columns...
                }
            }
            operationContext.recordFilter();
            return false; // No Columns Changed
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
