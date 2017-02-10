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

import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;

import java.io.Serializable;

/**
 * Created by dgomezferro on 4/4/14.
 */
public abstract class SpliceJoinFlatMapFunction<Op extends SpliceOperation, From, To>
		extends SpliceFlatMapFunction<Op,From,To> implements Serializable {
    public JoinOperation op = null;
    public boolean initialized = false;
    public int numberOfColumns = 0;
    public ExecutionFactory executionFactory;

    public SpliceJoinFlatMapFunction() {
	}

	public SpliceJoinFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
	}

    protected void checkInit () {
        if (!initialized) {
            op = (JoinOperation) getOperation();
            numberOfColumns = op.getLeftNumCols()+op.getRightNumCols();
            executionFactory = op.getExecutionFactory();
            initialized = true;
        }
    }
}