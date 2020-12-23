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
import com.splicemachine.derby.stream.iapi.OperationContext;

/**
 * Created by jleach on 4/24/15.
 */
public class ScrollInsensitiveFunction extends SpliceFunction<SpliceOperation, ExecRow, ExecRow> {
        protected SpliceOperation op;
        public ScrollInsensitiveFunction() {
            super();
        }
        protected boolean initialized = false;

        public ScrollInsensitiveFunction(OperationContext<SpliceOperation> operationContext) {
            super(operationContext);
        }

        @Override
        public ExecRow call(ExecRow execRow) throws Exception {
            if (!initialized) {
                op = (SpliceOperation) getOperation();
                initialized = true;
            }
            this.operationContext.recordRead();
            op.setCurrentRow(execRow);
            this.operationContext.recordProduced();
            return execRow;
        }

    @Override
    public boolean hasNativeSparkImplementation() {
        return true;
    }
}
