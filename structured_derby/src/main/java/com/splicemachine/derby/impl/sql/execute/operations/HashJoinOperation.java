package com.splicemachine.derby.impl.sql.execute.operations;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.utils.SpliceLogUtils;

public class HashJoinOperation extends NestedLoopJoinOperation {
	private static Logger LOG = Logger.getLogger(HashJoinOperation.class);
	protected NestedLoopIterator nestedLoopIterator;
	
	static {
		nodeTypes = new ArrayList<NodeType>();
		nodeTypes.add(NodeType.MAP);	
		nodeTypes.add(NodeType.SCROLL);
	}
	
	public HashJoinOperation() {
		super();
	}
	public HashJoinOperation(NoPutResultSet leftResultSet,
			   int leftNumCols,
			   NoPutResultSet rightResultSet,
			   int rightNumCols,
			   Activation activation,
			   GeneratedMethod restriction,
			   int resultSetNumber,
			   boolean oneRowRightSide,
			   boolean notExistsRightSide,
			   double optimizerEstimatedRowCount,
			   double optimizerEstimatedCost,
			   String userSuppliedOptimizerOverrides) throws StandardException {
		super(leftResultSet, leftNumCols, rightResultSet, rightNumCols,activation, restriction, resultSetNumber, oneRowRightSide, notExistsRightSide, optimizerEstimatedRowCount, optimizerEstimatedCost, userSuppliedOptimizerOverrides);
		if (LOG.isTraceEnabled())
			LOG.trace("instantiate");
        init(SpliceOperationContext.newContext(activation));
        recordConstructorTime();
	}
	
	@Override
	public ExecRow getNextRowCore() throws StandardException {
		SpliceLogUtils.trace(LOG, "getNextRowCore");
		beginTime = getCurrentTimeMillis();
		if (nestedLoopIterator == null || !nestedLoopIterator.hasNext()) {
			if ( (leftRow = leftResultSet.getNextRowCore()) == null) {
				mergedRow = null;
				setCurrentRow(mergedRow);
				return mergedRow;
			} else {
				rowsSeenLeft++;
				nestedLoopIterator = new NestedLoopIterator(leftRow);
				getNextRowCore();
			}
		}
		SpliceLogUtils.trace(LOG, "getNextRowCore loop iterate next ");		
		nextTime += getElapsedMillis(beginTime);
		rowsReturned++;
		return nestedLoopIterator.next();
	}

	@Override
	public void init(SpliceOperationContext context) throws StandardException{
		SpliceLogUtils.trace(LOG, "init");
		super.init(context);
		mergedRow = activation.getExecutionFactory().getValueRow(leftNumCols + rightNumCols);
		rightResultSet.init(context);
	}
	
	
	protected void getMergedRow(ExecRow leftRow, ExecRow rightRow) {
		SpliceLogUtils.trace(LOG, "getMergedRow with leftRow %s, right row %s",leftRow,rightRow);
		int colInCtr;
		int colOutCtr;
		int leftNumCols;
		int rightNumCols;
		/* Reverse left and right for return of row if this was originally
		 * a right outer join.  (Result columns ordered according to
		 * original query.)
		 */

		leftNumCols = this.leftNumCols;
		rightNumCols = this.rightNumCols;

		/* Merge the rows, doing just in time allocation for mergedRow.
		 * (By convention, left Row is to left of right Row.)
		 */
		try {
			for (colInCtr = 1, colOutCtr = 1; colInCtr <= leftNumCols;colInCtr++, colOutCtr++) {
				DataValueDescriptor src_col = leftRow.getColumn(colInCtr);
				// Clone the value if it is represented by a stream (DERBY-3650).
				if (src_col != null && src_col.hasStream()) {
					src_col = src_col.cloneValue(false);
				}
				mergedRow.setColumn(colOutCtr, src_col);
			}
			for (colInCtr = 1; colInCtr <= rightNumCols;colInCtr++, colOutCtr++) {
				DataValueDescriptor src_col = rightRow.getColumn(colInCtr);
				// Clone the value if it is represented by a stream (DERBY-3650).
				if (src_col != null && src_col.hasStream()) {
					src_col = src_col.cloneValue(false);
				}
				mergedRow.setColumn(colOutCtr, src_col);
			}
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "Error merging rows", e);
		}
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "final mergedRow %s",mergedRow);
	}
	@Override
	public NoPutResultSet executeScan() throws StandardException {
		SpliceLogUtils.trace(LOG, "executeScan");
		final List<SpliceOperation> operationStack = new ArrayList<SpliceOperation>();
		this.generateLeftOperationStack(operationStack);
		SpliceOperation regionOperation = operationStack.get(0);
		final RowProvider rowProvider;
		final ExecRow template = getExecRowDefinition();
		if (regionOperation.getNodeTypes().contains(NodeType.REDUCE) && this != regionOperation) {
			rowProvider = regionOperation.getReduceRowProvider(this,getRowEncoder().getDual(template));
		} else {
			rowProvider =regionOperation.getMapRowProvider(this,getRowEncoder().getDual(template));
		}
		return new SpliceNoPutResultSet(activation,this, rowProvider);
	}

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, RowDecoder decoder) throws StandardException {
        //TODO -sf- is this right?
        return getRightResultSet().getMapRowProvider(top, decoder);
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, RowDecoder decoder) throws StandardException {
        return getLeftOperation().getReduceRowProvider(top,decoder);
    }

    @Override
	public ExecRow getExecRowDefinition() throws StandardException {
		SpliceLogUtils.trace(LOG, "getExecRowDefinition");
		getMergedRow(((SpliceOperation)this.leftResultSet).getExecRowDefinition(),((SpliceOperation)this.rightResultSet).getExecRowDefinition());
		return mergedRow;
	}

	
	class NestedLoopIterator implements Iterator<ExecRow> {
		protected ExecRow leftRow;
		protected NoPutResultSet probeResultSet;
		NestedLoopIterator(ExecRow leftRow) throws StandardException {
			SpliceLogUtils.trace(LOG, "NestedLoopIterator instantiated with leftRow %s",leftRow);
			this.leftRow = leftRow;
			probeResultSet = ((SpliceOperation) getRightResultSet()).executeProbeScan();
			probeResultSet.openCore();
		}

		
		@Override
		public boolean hasNext() {
			SpliceLogUtils.trace(LOG, "hasNext called");
			try {
				ExecRow rightRow;
				if ( (rightRow = probeResultSet.getNextRowCore()) != null) {
					SpliceLogUtils.trace(LOG, "right has result %s",rightRow);
					rowsSeenRight++;
					getMergedRow(leftRow,rightRow);	
				} else {
					SpliceLogUtils.trace(LOG, "already has seen row and no right result");
					close();
					return false;
				}
				if (restriction != null) {
					DataValueDescriptor restrictBoolean = (DataValueDescriptor) restriction.invoke(activation);
					if ((! restrictBoolean.isNull()) && restrictBoolean.getBoolean()) {
						SpliceLogUtils.trace(LOG, "restricted row %s",mergedRow);
						hasNext();
					}
				}
			} catch (StandardException e) {
				SpliceLogUtils.logAndThrowRuntime(LOG, "hasNext Failed", e);
				try {
					close();
				} catch (StandardException e1) {
					SpliceLogUtils.logAndThrowRuntime(LOG, "close Failed", e1);
				}
				return false;
			}
			return true;
		}

		@Override
		public ExecRow next() {
			SpliceLogUtils.trace(LOG, "next row=%s",mergedRow);
			return mergedRow;
		}

		@Override
		public void remove() {
			SpliceLogUtils.trace(LOG, "remove");
		}
		public void close() throws StandardException {
			SpliceLogUtils.trace(LOG, "close in HashJoin");
			beginTime = getCurrentTimeMillis();
			if (!isOpen)
				return;
			SpliceLogUtils.trace(LOG, "close, closing probe result set");
			probeResultSet.close();
			isOpen = false;
			closeTime += getElapsedMillis(beginTime);
		}
	}

}
