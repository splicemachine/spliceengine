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
import com.splicemachine.db.iapi.types.HBaseRowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;

/**
 * Created by jleach on 4/24/15.
 */
public class ScrollInsensitiveFunction extends SpliceFunction<SpliceOperation, ExecRow, ExecRow> {
        protected SpliceOperation op;
        private BaseRowLocatorFunction<SpliceOperation> locatorFunction;
        public ScrollInsensitiveFunction() {
            super();
            locatorFunction = new BaseRowLocatorFunction<>();
        }
        protected boolean initialized = false;

        public ScrollInsensitiveFunction(OperationContext<SpliceOperation> operationContext) {
            super(operationContext);
            locatorFunction = new BaseRowLocatorFunction<>();
        }

        @Override
        public ExecRow call(ExecRow execRow) throws Exception {
            if (!initialized) {
                op = getOperation();
                initialized = true;
            }
            this.operationContext.recordRead();
            op.setCurrentRow(execRow);
            op.setCurrentRowLocation(execRow == null ? null : new HBaseRowLocation(execRow.getKey()));
            this.operationContext.recordProduced();
            if(execRow != null) {
                if (!op.isForUpdate()) {
                    execRow.setKey(null);
                    execRow.setBaseRowCols(null);
                } else {
                    ExecRow baseRow = locatorFunction.apply(operationContext);
                    op.setCurrentBaseRowLocation(baseRow);
                    execRow.setBaseRowCols(baseRow.getRowArray());
                }
            }
            return execRow;
        }

    @Override
    public boolean hasNativeSparkImplementation() {
        return true;
    }
}
