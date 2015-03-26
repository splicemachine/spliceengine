package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.Qualifier;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.utils.SpliceLogUtils;

public class NestedLoopLeftOuterJoinOperation extends NestedLoopJoinOperation {
		private static Logger LOG = Logger.getLogger(NestedLoopLeftOuterJoinOperation.class);
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
				try {
						init(SpliceOperationContext.newContext(activation));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
				recordConstructorTime();
		}

		@Override
		public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				SpliceLogUtils.trace(LOG, "nextRow");
                if (rightResultSetUniqueSequenceID == null) {
                    rightResultSetUniqueSequenceID = rightResultSet.getUniqueSequenceID();
                }

                if (nestedLoopIterator == null) {
						if ( (leftRow = leftResultSet.nextRow(spliceRuntimeContext)) == null) {
								mergedRow = null;
								setCurrentRow(mergedRow);
								return mergedRow;
						} else {
								nestedLoopIterator = new NestedLoopLeftOuterIterator(leftRow,isHash,spliceRuntimeContext, true, false);
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
								nestedLoopIterator = new NestedLoopLeftOuterIterator(leftRow,isHash,spliceRuntimeContext, true, false);
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
		public void init(SpliceOperationContext context) throws StandardException, IOException {
				SpliceLogUtils.trace(LOG, "init");
        super.init(context);
        emptyRightRowsReturned = 0;
        emptyRowFun = (emptyRowFunMethodName == null) ? null :
                context.getPreparedStatement().getActivationClass().getMethod(emptyRowFunMethodName);
		}

		@Override
		public String prettyPrint(int indentLevel) {
				return "LeftOuter"+super.prettyPrint(indentLevel);
		}

    @Override
    protected NestedLoopIterator createNestedLoopIterator(ExecRow leftRow, boolean hash, SpliceRuntimeContext spliceRuntimeContext, boolean showStatementInfo, boolean cloneResults) throws StandardException, IOException {
        return new NestedLoopLeftOuterIterator(leftRow, hash, spliceRuntimeContext, showStatementInfo, cloneResults);
    }

    private class NestedLoopLeftOuterIterator extends NestedLoopIterator{
				private boolean seenRow = false;

				NestedLoopLeftOuterIterator(ExecRow leftRow, boolean hash,SpliceRuntimeContext context, boolean showStatementInfo, boolean cloneResults) throws StandardException, IOException {
						super(leftRow, hash, true, rightResultSetUniqueSequenceID, context, showStatementInfo, cloneResults);
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
