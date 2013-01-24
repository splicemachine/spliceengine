package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.log4j.Logger;
import com.splicemachine.derby.utils.JoinSideExecRow;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;

public class MergeSortLeftOuterJoinOperation extends MergeSortJoinOperation {
	private static Logger LOG = Logger.getLogger(MergeSortLeftOuterJoinOperation.class);
	protected String emptyRowFunMethodName;
	protected boolean wasRightOuterJoin;
	protected GeneratedMethod emptyRowFun;
	
	public MergeSortLeftOuterJoinOperation() {
		super();
	}
	
	public MergeSortLeftOuterJoinOperation(
			NoPutResultSet leftResultSet,
			int leftNumCols,
			NoPutResultSet rightResultSet,
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
		if (rightIterator!= null && rightIterator.hasNext()) {
			currentRow = JoinUtils.getMergedRow(leftRow, rightIterator.next(), wasRightOuterJoin, this.rightNumCols, this.leftNumCols, mergedRow);
			this.setCurrentRow(currentRow);
			SpliceLogUtils.trace(LOG, "current row returned %s",currentRow);
			return currentRow;
		}
		if (!serverProvider.hasNext()) {
			SpliceLogUtils.trace(LOG, "serverProvider exhausted");
			return null;
		}
		JoinSideExecRow joinRow = serverProvider.nextJoinRow();
		SpliceLogUtils.trace(LOG, "joinRow=%s",joinRow);
		if (joinRow == null) {
			SpliceLogUtils.trace(LOG, "serverProvider returned null rows");
			this.setCurrentRow(null);
			return null;
		}
		
		if (joinRow.getJoinSide().ordinal() == JoinSide.RIGHT.ordinal()) { // Right Side
			rightHash = joinRow.getHash();
			if (joinRow.sameHash(priorHash)) {
				SpliceLogUtils.trace(LOG, "adding additional right=%s",joinRow);
				rights.add(joinRow.getRow());
			} else {
				resetRightSide();
				SpliceLogUtils.trace(LOG, "adding initial right=%s",joinRow);
				rights.add(joinRow.getRow());
				priorHash = joinRow.getHash();
			}
			return getNextRowCore();
		} else { // Left Side
			leftRow = joinRow.getRow();
			if (joinRow.sameHash(priorHash)) {
				if (joinRow.sameHash(rightHash)) {
					SpliceLogUtils.trace(LOG, "initializing iterator with rights for left=%s",joinRow);
					rightIterator = rights.iterator();
					currentRow = JoinUtils.getMergedRow(leftRow, rightIterator.next(), wasRightOuterJoin, this.rightNumCols, this.leftNumCols, mergedRow);
					this.setCurrentRow(currentRow);
					SpliceLogUtils.trace(LOG, "current row returned %s",currentRow);
					return currentRow;
				} else {
					SpliceLogUtils.trace(LOG, "right hash miss left=%s",joinRow);
					resetRightSide();	
					priorHash = joinRow.getHash();
					currentRow = JoinUtils.getMergedRow(leftRow, getEmptyRow(), wasRightOuterJoin, this.rightNumCols, this.leftNumCols, mergedRow); // Can this be null?
					this.setCurrentRow(currentRow);
					SpliceLogUtils.trace(LOG, "current row returned %s",currentRow);
					return currentRow;
				}
			} else {
				SpliceLogUtils.trace(LOG, "simple left emit=%s",joinRow);
				resetRightSide();
				priorHash = joinRow.getHash();
				currentRow = JoinUtils.getMergedRow(leftRow, getEmptyRow(), wasRightOuterJoin, this.leftNumCols, this.rightNumCols, mergedRow);
				this.setCurrentRow(currentRow);
				SpliceLogUtils.trace(LOG, "current row returned %s",currentRow);
				return currentRow;			
			}			
		}
	}
	
	@Override
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG, "init");
		super.init(context);
		try {
			emptyRowFun = (emptyRowFunMethodName == null) ? null : context.getPreparedStatement().getActivationClass().getMethod(emptyRowFunMethodName);
		} catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "Error initiliazing node", e);
		}
	}
	
	private ExecRow getEmptyRow () {
		try {
			return (ExecRow) emptyRowFun.invoke(activation);
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "Cannot Retrieve Empty Row", e);
		}
		return null;
	}
	
}