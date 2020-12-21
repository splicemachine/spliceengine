/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
import com.splicemachine.derby.impl.sql.execute.operations.IndexRowToBaseRowOperation;
import com.splicemachine.derby.impl.sql.execute.operations.ProjectRestrictOperation;
import com.splicemachine.derby.impl.sql.execute.operations.ScrollInsensitiveOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TableScanOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;

public class BaseRowLocatorFunction <Op extends SpliceOperation> extends SpliceFunction<Op, OperationContext<Op>,ExecRow> {

    public BaseRowLocatorFunction() {
        super();
    }

    @Override
    public ExecRow call(OperationContext<Op> operationContext) throws Exception {

        if(operationContext == null) {
            return null;
        }

        SpliceOperation op = operationContext.getOperation();
        while(op instanceof ProjectRestrictOperation || op instanceof ScrollInsensitiveOperation || op instanceof TableScanOperation || op instanceof IndexRowToBaseRowOperation) { // window operation also?
            if(op instanceof TableScanOperation || op instanceof IndexRowToBaseRowOperation) {
                return op.getCurrentRow();
            }
            if(op instanceof ProjectRestrictOperation) {
                op = ((ProjectRestrictOperation) op).getSource();
            } else {
                op = ((ScrollInsensitiveOperation) op).getSource();
            }
        }
        return null;
    }

    @Override
    public boolean hasNativeSparkImplementation() {
        return true;
    }
}
