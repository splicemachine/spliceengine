package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.utils.SpliceLogUtils;

public class NestedLoopLeftOuterJoinOperation extends NestedLoopJoinOperation {
		private static Logger LOG = Logger.getLogger(NestedLoopLeftOuterJoinOperation.class);
		protected String emptyRowFunMethodName;
		protected boolean wasRightOuterJoin;
		protected GeneratedMethod emptyRowFun;
		protected Qualifier[][] qualifierProbe;
		public int emptyRightRowsReturned = 0;

		public NestedLoopLeftOuterJoinOperation() {
				super();
		}

		public NestedLoopLeftOuterJoinOperation(
						SpliceOperation leftResultSet,
						int leftNumCols,
						SpliceOperation rightResultSet,
						int rightNumCols,
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
				super(leftResultSet, leftNumCols, rightResultSet, rightNumCols,
								activation, restriction, resultSetNumber,oneRowRightSide, notExistsRightSide,
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
				SpliceLogUtils.trace(LOG, "nextRow");
				if (nestedLoopIterator == null) {
						if ( (leftRow = leftResultSet.nextRow(spliceRuntimeContext)) == null) {
								mergedRow = null;
								setCurrentRow(mergedRow);
								return mergedRow;
						} else {
								nestedLoopIterator = new NestedLoopLeftOuterIterator(leftRow,isHash,spliceRuntimeContext);
								rowsSeenLeft++;
								return nextRow(spliceRuntimeContext);
						}
				}
				if(!nestedLoopIterator.hasNext()){
						nestedLoopIterator.close();

						if ( (leftRow = leftResultSet.nextRow(spliceRuntimeContext)) == null) {
								mergedRow = null;
								setCurrentRow(mergedRow);
								return mergedRow;
						} else {
								nestedLoopIterator = new NestedLoopLeftOuterIterator(leftRow,isHash,spliceRuntimeContext);
								rowsSeenLeft++;
								return nextRow(spliceRuntimeContext);
						}
				}

				SpliceLogUtils.trace(LOG, "nextRow loop iterate next ");
				ExecRow next = nestedLoopIterator.next();
				SpliceLogUtils.trace(LOG,"nextRow returning %s",next);
				setCurrentRow(next);
				rowsReturned++;
//		mergedRow=null;
				return next;

		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException{
				SpliceLogUtils.trace(LOG, "init");
				super.init(context);
				try {
						emptyRightRowsReturned = 0;
						emptyRowFun = (emptyRowFunMethodName == null) ? null :
										context.getPreparedStatement().getActivationClass().getMethod(emptyRowFunMethodName);
				} catch (StandardException e) {
						SpliceLogUtils.logAndThrowRuntime(LOG, "Error initiliazing node", e);
				}
		}

		@Override
		public String prettyPrint(int indentLevel) {
				return "LeftOuter"+super.prettyPrint(indentLevel);
		}

		private class NestedLoopLeftOuterIterator extends NestedLoopIterator{
				private boolean seenRow = false;

				NestedLoopLeftOuterIterator(ExecRow leftRow, boolean hash,SpliceRuntimeContext context) throws StandardException {
						super(leftRow, hash, true,context);
				}

				@Override
				protected void nonNullRight() {
						seenRow=true;
				}

				@Override
				protected ExecRow getEmptyRightRow() throws StandardException {
						if (seenRow) {
								SpliceLogUtils.trace(LOG, "already has seen row and no right result");
								probeResultSet.setCurrentRow(null);
								emptyRightRowsReturned++;
								close();
								return null;
						}
						rightRow = (ExecRow) emptyRowFun.invoke(activation);
						seenRow=true;
						return rightRow;
				}
		}
}
