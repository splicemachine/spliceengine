/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableIntHolder;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.Restriction;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;
import org.spark_project.guava.base.Strings;

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
		protected boolean serializeLeftResultSet = true;
		protected boolean serializeRightResultSet = true;
        protected ExecRow rightTemplate;
        protected ExecRow mergedRowTemplate;
        protected String emptyRowFunMethodName;
        public boolean wasRightOuterJoin = false;
        public boolean isOuterJoin = false;
		private Restriction mergeRestriction;

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
				String indent = "\n"+ Strings.repeat("\t", indentLevel);
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
		if (mergeRestriction == null) {
			mergeRestriction = Restriction.noOpRestriction;
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
		}
        return mergeRestriction;
    }

    public ExecRow getEmptyRow() throws StandardException {
        return rightTemplate;
    }

	public ExecRow getKeyRow(ExecRow row) throws StandardException {
		ExecRow keyRow = activation.getExecutionFactory().getValueRow(getLeftHashKeys().length);
		for (int i = 0; i < getLeftHashKeys().length; i++) {
			keyRow.setColumn(i + 1, row.getColumn(getLeftHashKeys()[i] + 1));
		}
		return keyRow;
	}

	public int[] getLeftHashKeys() {
		throw new UnsupportedOperationException();
	}

	public int[] getRightHashKeys() {
		throw new UnsupportedOperationException();
	}

	public long getSequenceId() {
		throw new UnsupportedOperationException("Not supported");
	}
}
