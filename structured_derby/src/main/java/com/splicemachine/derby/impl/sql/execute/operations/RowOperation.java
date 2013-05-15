package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.utils.SpliceLogUtils;


public class RowOperation extends SpliceBaseOperation implements CursorResultSet{
    private static final long serialVersionUID = 2l;
	private static Logger LOG = Logger.getLogger(RowOperation.class);
	protected int rowsReturned;
	protected boolean canCacheRow;
	protected boolean next = false;
	protected GeneratedMethod row;
	protected ExecRow		cachedRow;
    private String rowMethodName; //name of the row method for

	/**
	 * Required for serialization...
	 */
	public RowOperation() {
		
	}
	
	public RowOperation (
		Activation 	activation, 
		GeneratedMethod row, 
		boolean 		canCacheRow,
		int 			resultSetNumber,
		double 			optimizerEstimatedRowCount,
		double 			optimizerEstimatedCost ) throws StandardException {
		super(activation, resultSetNumber,optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.row = row;
		this.canCacheRow = canCacheRow;
        this.rowMethodName = row.getMethodName();
        init(SpliceOperationContext.newContext(activation));
        recordConstructorTime(); 
    }
	
	public RowOperation (
		Activation activation, 
		ExecRow constantRow, 
		boolean canCacheRow,
		int resultSetNumber,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost) throws StandardException {
		super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.cachedRow = constantRow;
		this.canCacheRow = canCacheRow;
		init(SpliceOperationContext.newContext(activation));
		recordConstructorTime(); 
    }


    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        super.init(context);
        next = false;
        if(row==null && rowMethodName!=null){
            if(rowMethodName!=null){
                try {
                    this.row = activation.getPreparedStatement().getActivationClass().getMethod(rowMethodName);
                } catch (StandardException e) {
                    SpliceLogUtils.logAndThrowRuntime(LOG,e);
                }
            }
        }
    }

    @Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		SpliceLogUtils.trace(LOG, "readExternal");
		super.readExternal(in);
		canCacheRow = in.readBoolean();
		next = in.readBoolean();
        if(in.readBoolean())
            rowMethodName = in.readUTF();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG, "writeExternal");
		super.writeExternal(out);
		out.writeBoolean(canCacheRow);
		out.writeBoolean(next);
        out.writeBoolean(row!=null);
        if(row!=null){
            out.writeUTF(rowMethodName);
        }
	}
	
	public void	openCore() throws StandardException  {
        super.openCore();
		SpliceLogUtils.trace(LOG, "openCore");
	   	next = false;
	}
	
	public ExecRow	getNextRowCore() throws StandardException {
		SpliceLogUtils.trace(LOG, "getNextRowCore, next="+next+",cachedRow="+cachedRow);
		currentRow = null;
		if (!next) {
			next = true;
			currentRow =getRow();
			rowsReturned++;
		}
		setCurrentRow(currentRow);
		return currentRow;
	}

	private ExecRow getRow() throws StandardException {
		SpliceLogUtils.trace(LOG,"getRow");
		if (cachedRow != null) {
			SpliceLogUtils.trace(LOG,"getRow,cachedRow="+cachedRow);
		    return cachedRow;
		}
		else if (row != null) {
		    ExecRow currentRow  = (ExecRow) row.invoke(activation);
		    if (canCacheRow) {
		        cachedRow = currentRow;
		    }
		    return currentRow;
		}
		return null;
	}
	/**
	 * This is not operating against a stored table,
	 * so it has no row location to report.
	 *
	 * @see CursorResultSet
	 *
	 * @return a null.
	 */
	public RowLocation getRowLocation() {
		SpliceLogUtils.logAndThrow(LOG, "RowResultSet used in positioned update/delete", new RuntimeException());
		return null;
	}

	/**
	 * This is not used in positioned update and delete,
	 * so just return a null.
	 *
	 * @see CursorResultSet
	 *
	 * @return a null.
	 */
	public ExecRow getCurrentRow() {
		SpliceLogUtils.logAndThrow(LOG, "RowResultSet used in positioned update/delete", new RuntimeException());
		return null;
	}

	@Override
	public List<NodeType> getNodeTypes() {
		return Collections.singletonList(NodeType.SCAN);
	}

	@Override
	public List<SpliceOperation> getSubOperations() {
		return Collections.emptyList();
	}
	

	@Override
	public SpliceOperation getLeftOperation() {
		return null;
	}

	@Override
	public String toString() {
		return "RowOp {cachedRow=" + cachedRow + "}";
	}

	@Override
	public NoPutResultSet executeScan() throws StandardException {
		SpliceLogUtils.trace(LOG, "executeScan");
		return new SpliceNoPutResultSet(activation,this, getMapRowProvider(this,getExecRowDefinition()));
	}
	
	@Override
	public RowProvider getMapRowProvider(SpliceOperation top,ExecRow rowTemplate) throws StandardException{
		SpliceLogUtils.trace(LOG, "getMapRowProvider,top="+top);
		top.init(SpliceOperationContext.newContext(activation));
		return RowProviders.sourceProvider(top, LOG);
	}
	
	@Override
	public RowProvider getReduceRowProvider(SpliceOperation top,ExecRow rowTemplate) throws StandardException {
        return getMapRowProvider(top,rowTemplate);
	}

	@Override
	public ExecRow getExecRowDefinition() {
		try {
			ExecRow row = getRow();
			SpliceLogUtils.trace(LOG,"execRowDefinition=%s",row);
			return row;
		} catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, e);
			return null;
		}
	}
	
	public int getRowsReturned() {
		return this.rowsReturned;
	}
	
	@Override
	public void	close() throws StandardException
	{
		SpliceLogUtils.trace(LOG,"close in RowOp");
	    
		beginTime = getCurrentTimeMillis();
		if (isOpen) {

			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
	    	clearCurrentRow();
	    	next = false;

			super.close();
		}
		
		closeTime += getElapsedMillis(beginTime);
	}
	
	@Override
	public long getTimeSpent(int type)
	{
		return constructorTime + openTime + nextTime + closeTime;
	}


}
