package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Strings;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.iapi.services.io.FormatableIntHolder;
import org.apache.derby.iapi.services.io.FormatableLongHolder;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
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
	protected GeneratedMethod restriction;
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
	
	public JoinOperation(NoPutResultSet leftResultSet,
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
		super(activation,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
		this.leftNumCols = leftNumCols;
		this.rightNumCols = rightNumCols;
		this.oneRowRightSide = oneRowRightSide;
		this.notExistsRightSide = notExistsRightSide;
		this.userSuppliedOptimizerOverrides = userSuppliedOptimizerOverrides;
		this.restrictionMethodName = (restriction == null) ? null : restriction.getMethodName();
		this.leftResultSet = (SpliceOperation) leftResultSet;
		this.leftResultSetNumber = leftResultSet.resultSetNumber();
		this.rightResultSet = (SpliceOperation) rightResultSet;
		recordConstructorTime();
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
//		if (LOG.isTraceEnabled())
//			LOG.trace("readExternal");
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
//		if (LOG.isTraceEnabled())
//			LOG.trace("writeExternal");
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
	public void init(SpliceOperationContext context) throws StandardException{
		SpliceLogUtils.trace(LOG, "init called");
		super.init(context);
            GenericStorablePreparedStatement statement = context.getPreparedStatement();
			restriction = (restrictionMethodName == null) ? null : statement.getActivationClass().getMethod(restrictionMethodName);
			leftResultSet.init(context);
			rightResultSet.init(context);
            SpliceLogUtils.trace(LOG,"leftResultSet=%s,rightResultSet=%s",leftResultSet,rightResultSet);
			leftRow = ((SpliceOperation)this.leftResultSet).getExecRowDefinition();
			rightRow = ((SpliceOperation)this.rightResultSet).getExecRowDefinition();
            SpliceLogUtils.trace(LOG,"leftRow=%s,rightRow=%s",leftRow,rightRow);
	}

    protected int[] generateHashKeys(int hashKeyItem, SpliceBaseOperation resultSet) {

        FormatableHashtable hashKeyInfo =  (FormatableHashtable) activation.getPreparedStatement().getSavedObject(hashKeyItem);

        FormatableArrayHolder fah = (FormatableArrayHolder) hashKeyInfo.get(JoinNode.HASH_KEYS_ARRAY_KEY);
        FormatableIntHolder[] fihArray = (FormatableIntHolder[]) fah.getArray(FormatableIntHolder.class);

        FormatableLongHolder tableNumber = (FormatableLongHolder) hashKeyInfo.get(JoinNode.TABLE_NUMBER_KEY);

        int[] rootAccessedCols = resultSet.getRootAccessedCols(tableNumber.getLong());
        int[] keyColumns = new int[fihArray.length];

        for(int i=0;i<fihArray.length;i++){
            keyColumns[i] = rootAccessedCols != null ? rootAccessedCols[fihArray[i].getInt()-1] : fihArray[i].getInt()-1;
        }
        return keyColumns;
	}

	@Override
	public void cleanup() {
		SpliceLogUtils.trace(LOG, "cleanup");
	}

	public SpliceOperation getRightResultSet() {
		return rightResultSet;
	}

	public SpliceOperation getLeftResultSet() {
		return leftResultSet;
	}
	
	@Override
	public SpliceOperation getLeftOperation() {
//		SpliceLogUtils.trace(LOG,"getLeftOperation");
		return leftResultSet;
	}

	@Override
	public SpliceOperation getRightOperation() {
//		SpliceLogUtils.trace(LOG,"getRightOperation");
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
		return String.format("JoinOperation {resultSetNumber=%d,left=%s,right=%s}",resultSetNumber,leftResultSet,rightResultSet);
	}
	@Override
	public void	close() throws StandardException
	{
		SpliceLogUtils.trace(LOG, "close in Join");
		if ( isOpen )
	    {
	        leftResultSet.close();
	        rightResultSet.close();
			super.close();
	    }
		
		leftRow = null;
		rightRow = null;
		mergedRow = null;
	}

    @Override
    public void openCore() throws StandardException {
        super.openCore();
        leftResultSet.openCore();
        rightResultSet.openCore();
    }

    @Override
     public int[] getRootAccessedCols(long tableNumber) {

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
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("leftNumCols:").append(leftNumCols).append(",")
                .append(indent).append("leftResultSetNumber:").append(leftResultSetNumber)
                .append(indent).append("leftResultSet:").append(leftResultSet.prettyPrint(indentLevel+1))
                .append(indent).append("rightNumCols:").append(rightNumCols).append(",")
                .append(indent).append("rightResultSet:").append(rightResultSet.prettyPrint(indentLevel+1))
                .append(indent).append("restrictionMethodName:").append(restrictionMethodName)
                .toString();
    }
}
