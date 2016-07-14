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

package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;

public class BroadcastLeftOuterJoinOperation extends BroadcastJoinOperation {
	private static Logger LOG = Logger.getLogger(BroadcastLeftOuterJoinOperation.class);
	protected SpliceMethod<ExecRow> emptyRowFun;
	protected ExecRow emptyRow;

	public BroadcastLeftOuterJoinOperation() {
		super();
	}
	
	public BroadcastLeftOuterJoinOperation(
			SpliceOperation leftResultSet,
			int leftNumCols,
			SpliceOperation rightResultSet,
			int rightNumCols,
			int leftHashKeyItem,
			int rightHashKeyItem,
			Activation activation,
			GeneratedMethod restriction,
			int resultSetNumber,
			GeneratedMethod emptyRowFun,
			boolean wasRightOuterJoin,
		    boolean oneRowRightSide,
		    boolean notExistsRightSide,
			    double optimizerEstimatedRowCount,
			double optimizerEstimatedCost,
			String userSuppliedOptimizerOverrides) throws StandardException {
                super(leftResultSet, leftNumCols, rightResultSet, rightNumCols, leftHashKeyItem, rightHashKeyItem,
                        activation, restriction, resultSetNumber, oneRowRightSide, notExistsRightSide,
                        optimizerEstimatedRowCount, optimizerEstimatedCost,userSuppliedOptimizerOverrides);
                SpliceLogUtils.trace(LOG, "instantiate");
                emptyRowFunMethodName = (emptyRowFun == null) ? null : emptyRowFun.getMethodName();
                this.wasRightOuterJoin = wasRightOuterJoin;
                this.isOuterJoin = true;
                init();
	}
	
    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        emptyRowFun = (emptyRowFunMethodName == null) ? null : new SpliceMethod<ExecRow>(emptyRowFunMethodName,activation);
    }

    @Override
    public ExecRow getEmptyRow () throws StandardException {
		if (emptyRow == null)
				emptyRow = emptyRowFun.invoke();
		return emptyRow;
	}

    @Override
    public String prettyPrint(int indentLevel) {
        return "LeftOuter"+super.prettyPrint(indentLevel);
    }

}