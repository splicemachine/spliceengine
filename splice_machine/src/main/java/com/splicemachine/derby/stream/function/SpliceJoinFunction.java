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

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 *
 */
public abstract class SpliceJoinFunction<Op extends SpliceOperation, From, To>
    extends SpliceFunction<Op,From,To> implements Serializable {
    private static final long serialVersionUID = 3988079974858059941L;
    protected JoinOperation op = null;
    protected boolean initialized = false;
    protected int numberOfColumns = 0;
    protected ExecutionFactory executionFactory;
	public SpliceJoinFunction() {
        super();
	}
	public SpliceJoinFunction(OperationContext<Op> operationContext) {
        super(operationContext);
	}

    @Nullable
    @Override
    public To apply(@Nullable From from) {
        return super.apply(from);
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