package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.utils.SpliceLogUtils;

public abstract class NoRowsOperation extends SpliceBaseOperation {
	private static Logger LOG = Logger.getLogger(NoRowsOperation.class);
	final Activation    activation;
    protected NoPutResultSet[] subqueryTrackingArray;
    protected boolean isOpen = true;
    
	protected static List<NodeType> parallelNodeTypes = Arrays.asList(NodeType.REDUCE,NodeType.SCAN);
	protected static List<NodeType> sequentialNodeTypes = Arrays.asList(NodeType.SCAN);
	
	private boolean isScan = true;
	
	public NoRowsOperation(Activation activation)  throws StandardException {
		super(activation,-1,0d,0d);
		this.activation = activation;
		init(SpliceOperationContext.newContext(activation));
		recordConstructorTime(); 
	}
	
	@Override
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG,"init with regionScanner %s",regionScanner);
		super.init(context);
		
		List<SpliceOperation> opStack = getOperationStack();
		boolean hasScan = false;
		for(SpliceOperation op:opStack){
			if(this!=op&&op.getNodeTypes().contains(NodeType.REDUCE)||op instanceof ScanOperation){
				hasScan =true;
				break;
			}
		}
		isScan = hasScan;
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		super.readExternal(in);
		isOpen = in.readBoolean();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeBoolean(isOpen);
	}
	
	@Override
	public SpliceOperation getLeftOperation() {
		return (SpliceOperation)null;
	}

	@Override
	public List<SpliceOperation> getSubOperations() {
		return new ArrayList<SpliceOperation>(0);
	}
	
	public final Activation getActivation() {
		SpliceLogUtils.trace(LOG, "getActivation");
		return activation;
	}
	
	@Override
	public List<NodeType> getNodeTypes() {
		return isScan ? parallelNodeTypes : sequentialNodeTypes;
	}
	
	@Override
	public ExecRow getNextRowCore() throws StandardException {
		return null;
	}

	protected void setup() throws StandardException {
		isOpen = true;
        StatementContext sc = activation.getLanguageConnectionContext().getStatementContext();
        
        if (sc == null) {
        	SpliceLogUtils.trace(LOG, "Cannot get StatementContext from Activation's lcc");
        	return;
        }
        sc.setTopResultSet(this, subqueryTrackingArray);

        // Pick up any materialized subqueries
        if (subqueryTrackingArray == null) {
            subqueryTrackingArray = sc.getSubqueryTrackingArray();
        }
	}
	
	@Override
	public boolean isClosed() {
		return !isOpen;
	}
	
	@Override
	public long getTimeSpent(int type)
	{
		return 0;
	}
	
	@Override
	public void close() {
		if (!isOpen)
			return;
		try {
			super.close();
			if (activation.isSingleExecution())
				activation.close();
	
		} catch (Exception e) {
			SpliceLogUtils.error(LOG, e);
		}
	}
}
