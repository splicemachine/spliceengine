package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;

import com.splicemachine.derby.utils.Exceptions;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;

public class BroadcastLeftOuterJoinOperation extends BroadcastJoinOperation {
	private static Logger LOG = Logger.getLogger(BroadcastLeftOuterJoinOperation.class);
	protected SpliceMethod<ExecRow> emptyRowFun;
	protected ExecRow emptyRow;

    { isOuterJoin = true; }

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
			try {
					init(SpliceOperationContext.newContext(activation));
			} catch (IOException e) {
					throw Exceptions.parseException(e);
			}
			recordConstructorTime();
	}
	
    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        emptyRowFun = (emptyRowFunMethodName == null) ? null : new SpliceMethod<ExecRow>(emptyRowFunMethodName,activation);
    }

    @Override
    protected ExecRow getEmptyRow () throws StandardException {
		if (emptyRow == null)
				emptyRow = emptyRowFun.invoke();
		return emptyRow;
	}

    @Override
    public String prettyPrint(int indentLevel) {
        return "LeftOuter"+super.prettyPrint(indentLevel);
    }

}