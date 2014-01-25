package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Strings;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.iapi.services.io.FormatableIntHolder;
import org.apache.derby.iapi.services.io.FormatableLongHolder;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.derby.impl.sql.compile.JoinNode;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.utils.SpliceLogUtils;

public abstract class JoinOperation extends SpliceBaseOperation {
    private static final long serialVersionUID = 2l;

	private static Logger LOG = Logger.getLogger(JoinOperation.class);
	protected int leftNumCols;
	protected int rightNumCols;
	protected boolean oneRowRightSide;
	protected boolean notExistsRightSide;
	protected String userSuppliedOptimizerOverrides;
	protected String restrictionMethodName;
	protected SpliceOperation rightResultSet;
	protected SpliceOperation leftResultSet;
	protected SpliceMethod<DataValueDescriptor> restriction;
	protected ExecRow leftRow;
	protected ExecRow rightRow;
	protected ExecRow mergedRow;
	protected int leftResultSetNumber;
	
	public int rowsSeenLeft;
	public int rowsSeenRight;
	public long restrictionTime;
	public int rowsReturned;
	
	public JoinOperation() {
		super();
	}
	
	public JoinOperation(SpliceOperation leftResultSet,
			   int leftNumCols,
			   SpliceOperation rightResultSet,
			   int rightNumCols,
			   Activation activation,
			   GeneratedMethod restriction,
			   int resultSetNumber,
			   boolean oneRowRightSide,
			   boolean notExistsRightSide,
			   double optimizerEstimatedRowCount,
			   double optimizerEstimatedCost,
			   String userSuppliedOptimizerOverrides) throws StandardException {
		super(activation,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
		this.leftNumCols = leftNumCols;
		this.rightNumCols = rightNumCols;
		this.oneRowRightSide = oneRowRightSide;
		this.notExistsRightSide = notExistsRightSide;
		this.userSuppliedOptimizerOverrides = userSuppliedOptimizerOverrides;
		this.restrictionMethodName = (restriction == null) ? null : restriction.getMethodName();
		this.leftResultSet = leftResultSet;
		this.leftResultSetNumber = leftResultSet.resultSetNumber();
		this.rightResultSet = rightResultSet;
		recordConstructorTime();
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		leftResultSetNumber = in.readInt();
		leftNumCols = in.readInt();
		rightNumCols = in.readInt();
		restrictionMethodName = readNullableString(in);
		userSuppliedOptimizerOverrides = readNullableString(in);
		oneRowRightSide = in.readBoolean();
		notExistsRightSide = in.readBoolean();
		leftResultSet = (SpliceOperation) in.readObject();
		rightResultSet = (SpliceOperation) in.readObject();
		rowsSeenLeft = in.readInt();
		rowsSeenRight = in.readInt();
		rowsReturned = in.readInt();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeInt(leftResultSetNumber);
		out.writeInt(leftNumCols);
		out.writeInt(rightNumCols);
		writeNullableString(restrictionMethodName, out);
		writeNullableString(userSuppliedOptimizerOverrides, out);
		out.writeBoolean(oneRowRightSide);
		out.writeBoolean(notExistsRightSide);	
		out.writeObject((SpliceOperation) leftResultSet);
		out.writeObject((SpliceOperation) rightResultSet);
		out.writeInt(rowsSeenLeft);
		out.writeInt(rowsSeenRight);
		out.writeInt(rowsReturned);
	}
	@Override
	public List<SpliceOperation> getSubOperations() {
		SpliceLogUtils.trace(LOG, "getSubOperations");
		List<SpliceOperation> operations = new ArrayList<SpliceOperation>();
		operations.add((SpliceOperation) leftResultSet);
		operations.add((SpliceOperation) rightResultSet);
		return operations;
	}

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        SpliceLogUtils.trace(LOG, "init called");
        super.init(context);
				restriction = restrictionMethodName == null ?
                        null : new SpliceMethod<DataValueDescriptor>(restrictionMethodName, activation);
        leftResultSet.init(context);
        rightResultSet.init(context);
        SpliceLogUtils.trace(LOG, "leftResultSet=%s,rightResultSet=%s", leftResultSet, rightResultSet);
        leftRow = ((SpliceOperation) this.leftResultSet).getExecRowDefinition();
        rightRow = ((SpliceOperation) this.rightResultSet).getExecRowDefinition();
        mergedRow = activation.getExecutionFactory().getValueRow(leftNumCols + rightNumCols);
        SpliceLogUtils.trace(LOG, "leftRow=%s,rightRow=%s", leftRow, rightRow);
    }

    protected int[] generateHashKeys(int hashKeyItem) {
        FormatableIntHolder[] fihArray = (FormatableIntHolder[]) activation.getPreparedStatement().getSavedObject(hashKeyItem);
        int[] cols = new int[fihArray.length];
        for (int i = 0, s = fihArray.length; i < s; i++){
            cols[i] = fihArray[i].getInt();
        }
        return cols;
	}

	public SpliceOperation getRightResultSet() {
		return rightResultSet;
	}

	public SpliceOperation getLeftResultSet() {
		return leftResultSet;
	}
	
	@Override
	public SpliceOperation getLeftOperation() {
		return leftResultSet;
	}

	@Override
	public SpliceOperation getRightOperation() {
		return rightResultSet;
	}

	public int getLeftNumCols() {
		return this.leftNumCols;
	}
	
	public int getRightNumCols() {
		return this.rightNumCols;
	}
	
	public boolean isOneRowRightSide() {
		return this.oneRowRightSide;
	}
	
	public String getUserSuppliedOptimizerOverrides() {
		return this.userSuppliedOptimizerOverrides;
	}

    @Override
    public boolean isReferencingTable(long tableNumber){
        return leftResultSet.isReferencingTable(tableNumber) || rightResultSet.isReferencingTable(tableNumber);
    }
	
	@Override
	public String toString() {
		return String.format("JoinOperation {resultSetNumber=%d,left=%s,right=%s}",operationInformation.getResultSetNumber(),leftResultSet,rightResultSet);
	}
	@Override
	public void	close() throws StandardException, IOException {
			super.close();
//		SpliceLogUtils.trace(LOG, "close in Join");
//		if ( isOpen )
//	    {
	        leftResultSet.close();
	        rightResultSet.close();
//	    }
		
		leftRow = null;
		rightRow = null;
		mergedRow = null;
	}

    @Override
    public void open() throws StandardException,IOException {
        super.open();
        leftResultSet.open();
        rightResultSet.open();
    }

    @Override
     public int[] getRootAccessedCols(long tableNumber) throws StandardException {

        int[] rootCols = null;

        if(leftResultSet.isReferencingTable(tableNumber)){
            rootCols = leftResultSet.getRootAccessedCols(tableNumber);
        }else if(rightResultSet.isReferencingTable(tableNumber)){
            int leftCols = getLeftNumCols();
            int[] rightRootCols = rightResultSet.getRootAccessedCols(tableNumber);
            rootCols = new int[rightRootCols.length];

            for(int i=0; i<rightRootCols.length; i++){
                rootCols[i] = rightRootCols[i] + leftCols;
            }

        }

        return rootCols;
     }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+Strings.repeat("\t",indentLevel);

        return new StringBuilder()
                .append(indent).append("resultSetNumber:").append(operationInformation.getResultSetNumber())
                .append(indent).append("leftNumCols:").append(leftNumCols).append(",")
                .append(indent).append("leftResultSetNumber:").append(leftResultSetNumber)
                .append(indent).append("leftResultSet:").append(leftResultSet.prettyPrint(indentLevel+1))
                .append(indent).append("rightNumCols:").append(rightNumCols).append(",")
                .append(indent).append("rightResultSet:").append(rightResultSet.prettyPrint(indentLevel+1))
                .append(indent).append("restrictionMethodName:").append(restrictionMethodName)
                .toString();
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        return leftResultSet.getMapRowProvider(top, rowDecoder, spliceRuntimeContext);
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        return leftResultSet.getReduceRowProvider(top, rowDecoder, spliceRuntimeContext);
    }

		@Override
		public ExecRow getExecRowDefinition() throws StandardException {
				if(mergedRow==null)
						mergedRow = activation.getExecutionFactory().getValueRow(leftNumCols+rightNumCols);
				JoinUtils.getMergedRow(this.leftResultSet.getExecRowDefinition(), this.rightResultSet.getExecRowDefinition(),false,rightNumCols,leftNumCols,mergedRow);
				return mergedRow;
		}

		@Override
		public NoPutResultSet executeScan(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				SpliceLogUtils.trace(LOG, "executeScan");
				return new SpliceNoPutResultSet(activation,this, getReduceRowProvider(this, OperationUtils.getPairDecoder(this, spliceRuntimeContext), spliceRuntimeContext));
		}
}
