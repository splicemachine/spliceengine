package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;

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
		private static List<NodeType> sequentialNodeTypes = Arrays.asList(NodeType.SCAN);

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
				init(SpliceOperationContext.newContext(activation));
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
		public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
				SpliceRuntimeContext spliceLeftRuntimeContext = runtimeContext.copy();
				spliceLeftRuntimeContext.addLeftRuntimeContext(resultSetNumber);
				SpliceRuntimeContext spliceRightRuntimeContext = runtimeContext.copy();
				spliceRightRuntimeContext.addRightRuntimeContext(resultSetNumber);
				RowProvider leftProvider =firstResultSet.getMapRowProvider(this,OperationUtils.getPairDecoder(firstResultSet,spliceLeftRuntimeContext),spliceLeftRuntimeContext);
				RowProvider rightProvider = secondResultSet.getMapRowProvider(this,OperationUtils.getPairDecoder(secondResultSet,spliceRightRuntimeContext),spliceRightRuntimeContext);
				return new SpliceNoPutResultSet(activation,this,RowProviders.combine(leftProvider, rightProvider));
		}

		@Override
		public SpliceOperation getRightOperation() {
				return secondResultSet;
		}

		@Override
		public void open() throws StandardException, IOException {
				super.open();
				firstResultSet.open();
				secondResultSet.open();
		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException {
				super.init(context);
				firstResultSet.init(context);
				secondResultSet.init(context);
				startExecutionTime = System.currentTimeMillis();
		}

		@Override
		public List<NodeType> getNodeTypes() {
				return sequentialNodeTypes;
		}

		@Override
		public List<SpliceOperation> getSubOperations() {
				return Arrays.asList(firstResultSet,secondResultSet);
		}

		@Override
		public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				ExecRow row;
				SpliceRuntimeContext.Side side = spliceRuntimeContext.getPathSide(resultSetNumber);
				if(timer==null)
						timer = spliceRuntimeContext.newTimer();

				timer.startTiming();
				switch (side) {
						case LEFT:
								row = firstResultSet.nextRow(spliceRuntimeContext);
								break;
						case RIGHT:
								row = secondResultSet.nextRow(spliceRuntimeContext);
								break;
						case MERGED:
								row = firstResultSet.nextRow(spliceRuntimeContext);
								if(row==null)
										row = secondResultSet.nextRow(spliceRuntimeContext);
								break;
						default:
								throw new IllegalStateException("Unknown side state "+ side);
				}
				setCurrentRow(row);
				if(row!=null){
						timer.tick(1);
				}else{
						timer.stopTiming();
						stopExecutionTime = System.currentTimeMillis();
				}
				return row;
		}

		@Override
		protected void updateStats(OperationRuntimeStats stats) {
				if(timer!=null){
						stats.addMetric(OperationMetric.INPUT_ROWS,timer.getNumEvents());
				}
		}

		@Override
		protected int getNumMetrics() {
				return super.getNumMetrics()+1;
		}

		@Override
		public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				if(OperationUtils.isInMemory(this)){
//				if(firstResultSet instanceof RowOperation && secondResultSet instanceof RowOperation){
						spliceRuntimeContext.addPath(resultSetNumber, SpliceRuntimeContext.Side.MERGED);
						return firstResultSet.getMapRowProvider(top,decoder,spliceRuntimeContext);
				}

				SpliceRuntimeContext left = spliceRuntimeContext.copy();
				SpliceRuntimeContext right = spliceRuntimeContext.copy();
				left.addPath(resultSetNumber, SpliceRuntimeContext.Side.LEFT);
				right.addPath(resultSetNumber, SpliceRuntimeContext.Side.RIGHT);
				RowProvider firstProvider = firstResultSet.getMapRowProvider(top,decoder,left);
				RowProvider secondProvider = secondResultSet.getMapRowProvider(top,decoder,right);
				return RowProviders.combine(firstProvider, secondProvider);
		}

		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException {
				return getMapRowProvider(top,decoder,spliceRuntimeContext);
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
}
