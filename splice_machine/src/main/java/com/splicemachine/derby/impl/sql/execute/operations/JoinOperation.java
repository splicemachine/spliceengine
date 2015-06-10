package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import com.google.common.base.Strings;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableIntHolder;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;

/**
 *
 * There are 6 different relational processing paths for joins determined by the different valid combinations of these boolean
 * fields.
 *
 *  1.  isOuterJoin: True for outer join and false for inner join
 *      - True (outer): select * from foo left outer join foo2 on foo.col1 = foo2.col1;
 *      - False (inner): select * from foo inner join foo2 on foo.col1 = foo2.col1;
 *
 *  2.  antiJoin(notExistsRightSide): True if the right side creates an antijoin or false if it does not.
 *      - True (antiJoin): select * from foo where not exists (select * from foo2 where foo.col1 = foo2.col1);
 *      - False (join): select * from foo where exists (select * from foo2 where foo.col1 = foo2.col1);
 *
 *  3.  hasRestriction: True if the join has a restriction and false if it does not.
 *      - True (restriction): select * from foo, foo2 where foo.col1 = foo2.col1 and foo.col1+foo.col2 > 10;
 *      - False (no restriction): select * from foo, foo2 where foo.col1 = foo2.col1 and foo.col2 = foo2.col2;
 *
 * 2^3 would mean there are 8 permutations, however antiJoin cannot be used with an outerjoin which removes 2 permutations.
 *
 * The valid combinations would be the following:
 *
 * (inner,join,no restriction)
 * (inner,join,restriction)
 * (inner,antiJoin,restriction)
 * (inner,antiJoin,no restriction)
 * (outer,join,no restriction)
 * (outer,join,restriction)
 *
 * Each Implementation of joins will provide an explanation of how they implement each of the 6 processing paths.
 *
 * @see com.splicemachine.derby.impl.sql.execute.operations.BroadcastJoinOperation
 * @see com.splicemachine.derby.impl.sql.execute.operations.MergeSortJoinOperation
 * @see com.splicemachine.derby.impl.sql.execute.operations.MergeJoinOperation
 * @see com.splicemachine.derby.impl.sql.execute.operations.NestedLoopJoinOperation
 *
 * These implementations contain the processing logic for these placeholder operations.
 *
 * @see com.splicemachine.derby.impl.sql.execute.operations.BroadcastLeftOuterJoinOperation
 * @see com.splicemachine.derby.impl.sql.execute.operations.MergeSortLeftOuterJoinOperation
 * @see com.splicemachine.derby.impl.sql.execute.operations.MergeLeftOuterJoinOperation
 * @see com.splicemachine.derby.impl.sql.execute.operations.NestedLoopLeftOuterJoinOperation
 *
 *
 *
 */
public abstract class JoinOperation extends SpliceBaseOperation {
		private static final long serialVersionUID = 2l;

		private static Logger LOG = Logger.getLogger(JoinOperation.class);
		protected int leftNumCols;
		protected int rightNumCols;
		public boolean oneRowRightSide;
		public boolean notExistsRightSide;
		protected String userSuppliedOptimizerOverrides;
		protected String restrictionMethodName;
		protected SpliceOperation rightResultSet;
		protected SpliceOperation leftResultSet;
		protected SpliceMethod<DataValueDescriptor> restriction;
		protected ExecRow leftRow;
		protected ExecRow rightRow;
		public ExecRow mergedRow;
		protected int leftResultSetNumber;

		public int rowsSeenLeft;
		public int rowsSeenRight;
		public long restrictionTime;
		public int rowsReturned;
		protected boolean serializeLeftResultSet = true;
		protected boolean serializeRightResultSet = true;
        protected ExecRow rightTemplate;
        protected ExecRow mergedRowTemplate;
        protected String emptyRowFunMethodName;
        public boolean wasRightOuterJoin = false;
        public boolean isOuterJoin = false;

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
                wasRightOuterJoin = in.readBoolean();
                isOuterJoin = in.readBoolean();
                emptyRowFunMethodName = readNullableString(in);
				boolean readMerged=false;
				if(in.readBoolean()){
						leftResultSet = (SpliceOperation) in.readObject();
						serializeLeftResultSet=true;
				}else readMerged=true;
				if(in.readBoolean()){
						rightResultSet = (SpliceOperation) in.readObject();
						serializeRightResultSet=true;
				}else readMerged=true;
				if(readMerged){
						mergedRow = (ExecRow) in.readObject();
						leftRow = (ExecRow)in.readObject();
						rightRow = (ExecRow)in.readObject();
                        mergedRowTemplate = (ExecRow)in.readObject();
				}
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
                out.writeBoolean(wasRightOuterJoin);
                out.writeBoolean(isOuterJoin);
                writeNullableString(emptyRowFunMethodName, out);
				out.writeBoolean(serializeLeftResultSet);
				if(serializeLeftResultSet)
						out.writeObject(leftResultSet);
				out.writeBoolean(serializeRightResultSet);
				if(serializeRightResultSet)
						out.writeObject(rightResultSet);
				if(!serializeRightResultSet||!serializeLeftResultSet){					
					out.writeObject(mergedRow);
						out.writeObject(leftRow);
						out.writeObject(rightRow);
                        out.writeObject(mergedRowTemplate);
				}
				out.writeInt(rowsSeenLeft);
				out.writeInt(rowsSeenRight);
				out.writeInt(rowsReturned);
		}
		@Override
		public List<SpliceOperation> getSubOperations() {
				SpliceLogUtils.trace(LOG, "getSubOperations");
				List<SpliceOperation> operations = new ArrayList<SpliceOperation>();
				operations.add(leftResultSet);
				operations.add(rightResultSet);
				return operations;
		}

		@Override
		public void init(SpliceOperationContext context) throws IOException, StandardException {
				SpliceLogUtils.trace(LOG, "init called");
				super.init(context);
				restriction = restrictionMethodName == null ?
								null : new SpliceMethod<DataValueDescriptor>(restrictionMethodName, activation);
				if(leftResultSet!=null){
						leftResultSet.init(context);
						leftRow = this.leftResultSet.getExecRowDefinition();
				}
				if(rightResultSet!=null){
						rightResultSet.init(context);
						rightRow = this.rightResultSet.getExecRowDefinition();
				}
				if(mergedRow==null)
						mergedRow = activation.getExecutionFactory().getValueRow(leftNumCols + rightNumCols);
                if (rightTemplate == null)
                    rightTemplate = rightRow.getClone();
		}

		protected int[] generateHashKeys(int hashKeyItem) {
				FormatableIntHolder[] fihArray = (FormatableIntHolder[]) activation.getPreparedStatement().getSavedObject(hashKeyItem);
				int[] cols = new int[fihArray.length];
				for (int i = 0, s = fihArray.length; i < s; i++){
						cols[i] = fihArray[i].getInt();
				}
				return cols;
		}

		public SpliceOperation getLeftResultSet() {
				return leftResultSet;
		}

		@Override
		public SpliceOperation getLeftOperation() {
				return leftResultSet;
		}

        public SpliceOperation getRightResultSet() {
        return rightResultSet;
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
				return String.format("JoinOperation {resultSetNumber=%d,optimizerEstimatedCost=%f,optimizerEstimatedRowCount=%f,left=%s,right=%s}",operationInformation.getResultSetNumber(),optimizerEstimatedCost,optimizerEstimatedRowCount,leftResultSet,rightResultSet);
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
								.append(indent).append("optimizerEstimatedCost:").append(optimizerEstimatedCost).append(",")
								.append(indent).append("optimizerEstimatedRowCount:").append(optimizerEstimatedRowCount).append(",")								
								.append(indent).append("leftNumCols:").append(leftNumCols).append(",")
								.append(indent).append("leftResultSetNumber:").append(leftResultSetNumber)
								.append(indent).append("leftResultSet:").append(leftResultSet.prettyPrint(indentLevel+1))
								.append(indent).append("rightNumCols:").append(rightNumCols).append(",")
								.append(indent).append("rightResultSet:").append(rightResultSet.prettyPrint(indentLevel+1))
								.append(indent).append("restrictionMethodName:").append(restrictionMethodName)
								.toString();
		}

		@Override
        public ExecRow getExecRowDefinition() throws StandardException {
            if (mergedRowTemplate == null) {
                mergedRowTemplate = activation.getExecutionFactory().getValueRow(leftNumCols + rightNumCols);
                JoinUtils.getMergedRow(leftResultSet.getExecRowDefinition(), rightResultSet.getExecRowDefinition(),
                                          wasRightOuterJoin, mergedRowTemplate);
            }
            return mergedRowTemplate;
        }

    public Restriction getRestriction() {
        Restriction mergeRestriction = Restriction.noOpRestriction;
        if (restriction != null) {
            mergeRestriction = new Restriction() {
                @Override
                public boolean apply(ExecRow row) throws StandardException {
                    activation.setCurrentRow(row, resultSetNumber);
                    DataValueDescriptor shouldKeep = restriction.invoke();
                    return !shouldKeep.isNull() && shouldKeep.getBoolean();
                }
            };
        }
        return mergeRestriction;
    }

    public ExecRow getEmptyRow() throws StandardException {
        return rightTemplate;
    }
}
