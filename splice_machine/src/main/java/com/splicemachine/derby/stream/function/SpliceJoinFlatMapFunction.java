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