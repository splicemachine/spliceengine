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

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 4/24/15.
 */
public class ScrollInsensitiveFunction extends SpliceFunction<SpliceOperation, LocatedRow, LocatedRow> {
        protected SpliceOperation op;
        public ScrollInsensitiveFunction() {
            super();
        }
        protected boolean initialized = false;

        public ScrollInsensitiveFunction(OperationContext<SpliceOperation> operationContext) {
            super(operationContext);
        }

        @Override
        public LocatedRow call(LocatedRow locatedRow) throws Exception {
            if (!initialized) {
                op = (SpliceOperation) getOperation();
                initialized = true;
            }
            this.operationContext.recordRead();
            op.setCurrentLocatedRow(locatedRow);
            this.operationContext.recordProduced();
            return locatedRow;
        }

}
