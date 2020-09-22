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
import com.splicemachine.derby.impl.sql.execute.operations.RowOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;

/**
 * Created by jleach on 5/19/15.
 */
public class RowOperationFunction extends SpliceFunction<RowOperation,ExecRow,ExecRow> {

    public RowOperationFunction() {
        super();
    }

    public RowOperationFunction(OperationContext<RowOperation> operationContext) {
        super(operationContext);
    }

    @Override
    public ExecRow call(ExecRow o) throws Exception {
        RowOperation rowOp = operationContext.getOperation();
        ExecRow valueRow = rowOp.getRow();
        rowOp.setCurrentRow(valueRow);
        return valueRow;
    }

}
