package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.SpliceMethod;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;

import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;

public class MergeSortLeftOuterJoinOperation extends MergeSortJoinOperation {
    private static Logger LOG = Logger.getLogger(MergeSortLeftOuterJoinOperation.class);

		@SuppressWarnings("UnusedDeclaration")
		public MergeSortLeftOuterJoinOperation() {
				super();
		}

		public MergeSortLeftOuterJoinOperation(
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
				this.emptyRowFunMethodName = (emptyRowFun == null) ? null : emptyRowFun.getMethodName();
				this.wasRightOuterJoin = wasRightOuterJoin;
				try {
						init(SpliceOperationContext.newContext(activation));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
				recordConstructorTime();

		}

		@Override
		public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				return next(true, spliceRuntimeContext);
		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException, IOException {
				SpliceLogUtils.trace(LOG, "init");
				super.init(context);
				emptyRowFun = (emptyRowFunMethodName == null) ? null : new SpliceMethod<ExecRow>(emptyRowFunMethodName,context.getActivation());
		}

		@Override
		public String prettyPrint(int indentLevel) {
				return "LeftOuter"+super.prettyPrint(indentLevel);
		}

    @Override
    protected ExecRow getEmptyRow() throws StandardException {
        if (emptyRow == null)
            emptyRow = emptyRowFun.invoke();
        return emptyRow;
    }
}
