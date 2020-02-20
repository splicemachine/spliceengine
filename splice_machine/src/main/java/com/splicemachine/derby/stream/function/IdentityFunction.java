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
 *
 */

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;


public class IdentityFunction<Op extends SpliceOperation> extends SpliceFunction<Op, ExecRow, ExecRow> {
    public IdentityFunction() {
    }

    public IdentityFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public ExecRow call(ExecRow o) throws Exception {
        return o;
    }

    @Override
    public boolean hasNativeSparkImplementation() {
        return false;    // force materialization into derby's representation
    }
}
