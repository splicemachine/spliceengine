package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;

public class BroadcastLeftOuterJoinOperation extends BroadcastJoinOperation {
	private static Logger LOG = Logger.getLogger(BroadcastLeftOuterJoinOperation.class);
	protected String emptyRowFunMethodName;
	protected boolean wasRightOuterJoin;
	protected GeneratedMethod emptyRowFun;
	protected ExecRow emptyRow;
	
	public BroadcastLeftOuterJoinOperation() {
		super();
	}
	
	public BroadcastLeftOuterJoinOperation(
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
	public ExecRow getNextRowCore() throws StandardException {
		SpliceLogUtils.trace(LOG, "getNextRowCore");
		if (broadcastIterator == null)
			//broadcastIterator = new BroadcastNextRowIterator(true);
		if (broadcastIterator.hasNext()) {
			return broadcastIterator.next();
		} else {
			setCurrentRow(null);
			return null;
		}
		return null;
	}
	
	@Override
	public void init(SpliceOperationContext context) throws StandardException{
		SpliceLogUtils.trace(LOG, "init");
		super.init(context);
		try {
			emptyRowFun = (emptyRowFunMethodName == null) ? null : context.getPreparedStatement().getActivationClass().getMethod(emptyRowFunMethodName);
		} catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "Error initiliazing node", e);
		}
	}
	
	protected ExecRow getEmptyRow () {
		if (emptyRow ==null)
			try {
				emptyRow =  (ExecRow) emptyRowFun.invoke(activation);
			} catch (StandardException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return emptyRow;
	}	
}