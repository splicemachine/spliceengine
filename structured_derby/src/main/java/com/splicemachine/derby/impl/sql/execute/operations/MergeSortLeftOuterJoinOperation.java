package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.SpliceMethod;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;

public class MergeSortLeftOuterJoinOperation extends MergeSortJoinOperation { private static Logger LOG = Logger.getLogger(MergeSortLeftOuterJoinOperation.class);
		protected String emptyRowFunMethodName;

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
				init(SpliceOperationContext.newContext(activation));
				recordConstructorTime();

		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
				SpliceLogUtils.trace(LOG, "readExternal");
				super.readExternal(in);
				emptyRowFunMethodName = readNullableString(in);
				wasRightOuterJoin = in.readBoolean();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				SpliceLogUtils.trace(LOG, "writeExternal");
				super.writeExternal(out);
				writeNullableString(emptyRowFunMethodName, out);
				out.writeBoolean(wasRightOuterJoin);
		}

		@Override
		public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				return next(true, spliceRuntimeContext);
		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException{
				SpliceLogUtils.trace(LOG, "init");
				super.init(context);
				emptyRowFun = (emptyRowFunMethodName == null) ? null : new SpliceMethod<ExecRow>(emptyRowFunMethodName,context.getActivation());
		}

		@Override
		public String prettyPrint(int indentLevel) {
				return "LeftOuter"+super.prettyPrint(indentLevel);
		}

}