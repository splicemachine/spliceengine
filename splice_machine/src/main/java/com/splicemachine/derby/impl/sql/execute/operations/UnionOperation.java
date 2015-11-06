package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.function.SetCurrentLocatedRowFunction;
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
		/* Pull rows from firstResultSet, then rightResultSet*/
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
				try {
						init(SpliceOperationContext.newContext(activation));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
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
		    return null;
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

    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        return leftResultSet.getDataSet(dsp).union(rightResultSet.getDataSet(dsp))
                .map(new SetCurrentLocatedRowFunction<SpliceOperation>(dsp.createOperationContext(this)));
    }
}
