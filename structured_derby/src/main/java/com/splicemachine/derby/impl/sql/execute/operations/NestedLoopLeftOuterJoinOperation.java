package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.utils.SpliceLogUtils;

public class NestedLoopLeftOuterJoinOperation extends NestedLoopJoinOperation {
	private static Logger LOG = Logger.getLogger(NestedLoopLeftOuterJoinOperation.class);
	protected String emptyRowFunMethodName;
	protected boolean wasRightOuterJoin;
	protected GeneratedMethod emptyRowFun;
	protected Qualifier[][] qualifierProbe;
	protected NestedLoopLeftIterator nestedLoopLeftIterator;
	
	public NestedLoopLeftOuterJoinOperation() {
		super();
	}

	public NestedLoopLeftOuterJoinOperation(
			NoPutResultSet leftResultSet,
			int leftNumCols,
			NoPutResultSet rightResultSet,
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
	public ExecRow getNextRowCore() throws StandardException {
		SpliceLogUtils.trace(LOG, "getNextRowCore");
		if (nestedLoopIterator == null) {
			if ( (leftRow = leftResultSet.getNextRowCore()) == null) {
				mergedRow = null;
				setCurrentRow(mergedRow);
				return mergedRow;
			} else {
				nestedLoopIterator = new NestedLoopIterator(leftRow,isHash);
				return getNextRowCore();
			}
		}
		if(!nestedLoopIterator.hasNext()){
			nestedLoopIterator.close();

			if ( (leftRow = leftResultSet.getNextRowCore()) == null) {
				mergedRow = null;
				setCurrentRow(mergedRow);
				return mergedRow;
			} else {
				nestedLoopIterator = new NestedLoopIterator(leftRow,isHash);
				return getNextRowCore();
			}
		}

		SpliceLogUtils.trace(LOG, "getNextRowCore loop iterate next ");
		ExecRow next = nestedLoopIterator.next();
		SpliceLogUtils.trace(LOG,"getNextRowCore returning %s",next);
		setCurrentRow(next);
//		mergedRow=null;
		return next;

	}

	@Override
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG, "init");
		super.init(context);
		try {
			emptyRowFun = (emptyRowFunMethodName == null) ? null :
                                    context.getPreparedStatement().getActivationClass().getMethod(emptyRowFunMethodName);
		} catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "Error initiliazing node", e);
		}
	}
	
	@Override
	public ExecRow getExecRowDefinition() {
		SpliceLogUtils.trace(LOG, "getExecRowDefinition");
		mergedRow = JoinUtils.getMergedRow(((SpliceOperation)this.leftResultSet).getExecRowDefinition(),((SpliceOperation)this.rightResultSet).getExecRowDefinition(),wasRightOuterJoin,rightNumCols,leftNumCols,mergedRow);
		return mergedRow;
	}

	protected class NestedLoopLeftIterator implements Iterator<ExecRow> {
		protected ExecRow leftRow;
		protected NoPutResultSet probeResultSet;
		protected boolean seenRow;
		NestedLoopLeftIterator(ExecRow leftRow, boolean hash) throws StandardException {
			SpliceLogUtils.trace(LOG, "NestedLoopIterator instantiated with leftRow " + leftRow);
			this.leftRow = leftRow;
			if (hash)
				probeResultSet = ((SpliceOperation) getRightResultSet()).executeProbeScan();
			else
				probeResultSet = ((SpliceOperation) getRightResultSet()).executeScan();				
			probeResultSet.openCore();
		}
		
		@Override
		public boolean hasNext() {
			SpliceLogUtils.trace(LOG, "hasNext called");
			try {
				ExecRow rightRow;
				probeResultSet.clearCurrentRow();
				if ( (rightRow = probeResultSet.getNextRowCore()) != null) {
					probeResultSet.setCurrentRow(rightRow);
					SpliceLogUtils.trace(LOG, "right has result " + rightRow);
					mergedRow = JoinUtils.getMergedRow(leftRow,rightRow,wasRightOuterJoin,rightNumCols,leftNumCols,mergedRow);
				} else {
					if (seenRow) {
						SpliceLogUtils.trace(LOG, "already has seen row and no right result");
						probeResultSet.setCurrentRow(null);
						close();
						return false;
					}
					rightRow = (ExecRow) emptyRowFun.invoke(activation);
					mergedRow = JoinUtils.getMergedRow(leftRow,rightRow,wasRightOuterJoin,rightNumCols,leftNumCols,mergedRow);
				}						
				if (restriction != null) {
					DataValueDescriptor restrictBoolean = (DataValueDescriptor) restriction.invoke(activation);
					if ((! restrictBoolean.isNull()) && restrictBoolean.getBoolean()) {
						SpliceLogUtils.trace(LOG, "restricted row " + mergedRow);
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
			seenRow = true;
			return true;
		}

		@Override
		public ExecRow next() {
			SpliceLogUtils.trace(LOG, "next row=" + mergedRow);
			return mergedRow;
		}

		@Override
		public void remove() {
			SpliceLogUtils.trace(LOG, "remove");
		}
		public void close() throws StandardException {
			SpliceLogUtils.trace(LOG, "close, closing probe result set");
			probeResultSet.close();
		}
	}

}
