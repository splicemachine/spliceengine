package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
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
		/* Pull rows from firstResultSet, then secondResultSet*/
		public SpliceOperation firstResultSet;
		public SpliceOperation secondResultSet;
		public int rowsSeenLeft = 0;
		public int rowsSeenRight = 0;
		public int rowsReturned= 0;
		private Boolean isLeft = null;
	    protected static final String NAME = UnionOperation.class.getSimpleName().replaceAll("Operation","");

		@Override
		public String getName() {
				return NAME;
		}

		
		@SuppressWarnings("UnusedDeclaration")
		public UnionOperation(){
				super();
		}

		public UnionOperation(SpliceOperation firstResultSet,
													SpliceOperation secondResultSet,
													Activation activation,
													int resultSetNumber,
													double optimizerEstimatedRowCount,
													double optimizerEstimatedCost) throws StandardException{
				super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
				this.firstResultSet = firstResultSet;
				this.secondResultSet = secondResultSet;
				try {
						init(SpliceOperationContext.newContext(activation));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				super.readExternal(in);
				firstResultSet = (SpliceOperation)in.readObject();
				secondResultSet = (SpliceOperation)in.readObject();
				isLeft = null;
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				out.writeObject(firstResultSet);
				out.writeObject(secondResultSet);
		}

		@Override
		public ExecRow getExecRowDefinition() throws StandardException {
				if(isLeft == null || isLeft)
						return firstResultSet.getExecRowDefinition();
				else return secondResultSet.getExecRowDefinition();
		}

		@Override
		public SpliceOperation getLeftOperation() {
				return firstResultSet;
		}

		@Override
		public SpliceOperation getRightOperation() {
				return secondResultSet;
		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException, IOException {
				super.init(context);
				firstResultSet.init(context);
				secondResultSet.init(context);
				startExecutionTime = System.currentTimeMillis();
		}

		@Override
		public List<SpliceOperation> getSubOperations() {
				return Arrays.asList(firstResultSet,secondResultSet);
		}

		@Override
		public String toString() {
				return "UnionOperation{" +
								"left=" + firstResultSet +
								", right=" + secondResultSet +
								'}';
		}

		@Override
		public String prettyPrint(int indentLevel) {
				String indent = "\n"+ Strings.repeat("\t",indentLevel);

				return "Union:" + indent + "resultSetNumber:" + resultSetNumber
								+ indent + "firstResultSet:" + firstResultSet
								+ indent + "secondResultSet:" + secondResultSet;
		}

		@Override
		public int[] getRootAccessedCols(long tableNumber) throws StandardException {
				if(firstResultSet.isReferencingTable(tableNumber))
						return firstResultSet.getRootAccessedCols(tableNumber);
				else if(secondResultSet.isReferencingTable(tableNumber))
						return secondResultSet.getRootAccessedCols(tableNumber);

				return null;
		}

		@Override
		public boolean isReferencingTable(long tableNumber) {
				return firstResultSet.isReferencingTable(tableNumber) || secondResultSet.isReferencingTable(tableNumber);

		}

    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        return firstResultSet.getDataSet().union(secondResultSet.getDataSet());
    }

}
