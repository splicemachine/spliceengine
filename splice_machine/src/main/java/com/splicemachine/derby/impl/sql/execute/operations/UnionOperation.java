/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.function.SetCurrentLocatedRowFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import splice.com.google.common.base.Strings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * Unions come in two different forms: UNION and UNION ALL. In a normal UNION, there are no
 * duplicates allowed, while UNION ALL allows duplicates. In Derby practice, UNION is implemented as
 * a Distinct Sort over top of a Union Operation, while UNION ALL is just a Union operation directly. Thus,
 * from the standpoint of the Union Operation, no distinction needs to be made between UNION and UNION ALL, and
 * hence Unions can be treated as Scan-only operations (like TableScan or ProjectRestrict) WITH some special
 * circumstances--namely, that there are two result sets to localize against when Unions are used under
 * parallel operations.
 *
 * @author Scott Fines
 * Created on: 5/14/13
 */
public class UnionOperation extends SpliceBaseOperation {
		private static final long serialVersionUID = 1l;
		public SpliceOperation leftResultSet;
		public SpliceOperation rightResultSet;
	    protected static final String NAME = UnionOperation.class.getSimpleName().replaceAll("Operation","");

		@Override
		public String getName() {
				return NAME;
		}


		@SuppressWarnings("UnusedDeclaration")
		public UnionOperation(){
				super();
		}

		public UnionOperation(SpliceOperation leftResultSet,
													SpliceOperation rightResultSet,
													Activation activation,
													int resultSetNumber,
													double optimizerEstimatedRowCount,
													double optimizerEstimatedCost) throws StandardException{
				super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
				this.leftResultSet = leftResultSet;
				this.rightResultSet = rightResultSet;
				init();
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				super.readExternal(in);
				leftResultSet = (SpliceOperation)in.readObject();
				rightResultSet = (SpliceOperation)in.readObject();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				out.writeObject(leftResultSet);
				out.writeObject(rightResultSet);
		}

		@Override
		public ExecRow getExecRowDefinition() throws StandardException {
		    return leftResultSet.getExecRowDefinition();
		}

		@Override
		public SpliceOperation getLeftOperation() {
				return leftResultSet;
		}

		@Override
		public SpliceOperation getRightOperation() {
				return rightResultSet;
		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException, IOException {
				super.init(context);
				leftResultSet.init(context);
				rightResultSet.init(context);
		}

		@Override
		public List<SpliceOperation> getSubOperations() {
				return Arrays.asList(leftResultSet, rightResultSet);
		}

		@Override
		public String toString() {
				return "UnionOperation{" +
								"left=" + leftResultSet +
								", right=" + rightResultSet +
								'}';
		}

		@Override
		public String prettyPrint(int indentLevel) {
				String indent = "\n"+ Strings.repeat("\t",indentLevel);

				return "Union:" + indent + "resultSetNumber:" + resultSetNumber
								+ indent + "leftResultSet:" + leftResultSet
								+ indent + "rightResultSet:" + rightResultSet;
		}

		@Override
		public int[] getRootAccessedCols(long tableNumber) throws StandardException {
				if(leftResultSet.isReferencingTable(tableNumber))
						return leftResultSet.getRootAccessedCols(tableNumber);
				else if(rightResultSet.isReferencingTable(tableNumber))
						return rightResultSet.getRootAccessedCols(tableNumber);

				return null;
		}

		@Override
		public boolean isReferencingTable(long tableNumber) {
				return leftResultSet.isReferencingTable(tableNumber) || rightResultSet.isReferencingTable(tableNumber);

		}

    @SuppressWarnings("rawtypes")
    @Override
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException {
		if (!isOpen)
			throw new IllegalStateException("Operation is not open");

		OperationContext operationContext = dsp.createOperationContext(this);
		boolean useSpark = dsp.getType().equals(DataSetProcessor.Type.SPARK);

		DataSet<ExecRow> result;
		if (!useSpark) {
			ArrayList<DataSet<ExecRow>> datasetList = new ArrayList<>();
			collectUnionAllBranches(this, datasetList, dsp);
			DataSet<ExecRow> dataSet = dsp.getEmpty();
			operationContext.pushScope();
			result = dataSet
					.union(datasetList, operationContext)
					.map(new SetCurrentLocatedRowFunction<SpliceOperation>(operationContext), true);
			operationContext.popScope();
			return result;
		} else {
		        dsp.incrementOpDepth();
		        dsp.finalizeTempOperationStrings();
			DataSet<ExecRow> left = leftResultSet.getDataSet(dsp);
			dsp.finalizeTempOperationStrings();
			DataSet<ExecRow> right = rightResultSet.getDataSet(dsp);
		        dsp.decrementOpDepth();
			operationContext.pushScope();
			result = left
					.union(right, operationContext)
					.map(new SetCurrentLocatedRowFunction<SpliceOperation>(operationContext), true);
			operationContext.popScope();
		        handleSparkExplain(result, left, right, dsp);
		}
		return result;
    }

    /* flatten the nested union-all branches */
    public void collectUnionAllBranches(UnionOperation unionOperation,
									 ArrayList<DataSet<ExecRow>> datasetList,
									 DataSetProcessor dsp) throws StandardException {
		SpliceOperation left = unionOperation.getLeftOperation();
		if (left instanceof UnionOperation && !(left instanceof RecursiveUnionOperation))
			collectUnionAllBranches((UnionOperation)left, datasetList, dsp);
		else
			datasetList.add(left.getDataSet(dsp));

		SpliceOperation right = unionOperation.getRightOperation();
		if (right instanceof UnionOperation && !(right instanceof RecursiveUnionOperation))
			collectUnionAllBranches((UnionOperation)right, datasetList, dsp);
		else
			datasetList.add(right.getDataSet(dsp));

		return;
	}
}
