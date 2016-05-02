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
import com.splicemachine.pipeline.exception.Exceptions;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.spark.api.java.JavaRDD;

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
		public int rowsSeenLeft = 0;
		public int rowsSeenRight = 0;
		public int rowsReturned= 0;
		private static List<NodeType> sequentialNodeTypes = Arrays.asList(NodeType.SCAN);
		private SpliceRuntimeContext.Side side = SpliceRuntimeContext.Side.LEFT;

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
			switch (side) {
				case LEFT:
					return leftResultSet.getExecRowDefinition();
				case RIGHT:
					return rightResultSet.getExecRowDefinition();
				case MERGED: {
					ExecRow leftExecRow = leftResultSet.getExecRowDefinition();
					return leftExecRow != null ? leftExecRow : rightResultSet.getExecRowDefinition();
				}
				default:
					return null;
			}
		}

		@Override
		public SpliceOperation getLeftOperation() {
				return leftResultSet;
		}

		@Override
		public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
				SpliceRuntimeContext spliceLeftRuntimeContext = runtimeContext.copy();
				spliceLeftRuntimeContext.addLeftRuntimeContext(resultSetNumber);
				SpliceRuntimeContext spliceRightRuntimeContext = runtimeContext.copy();
				spliceRightRuntimeContext.addRightRuntimeContext(resultSetNumber);
				try {
						RowProvider leftProvider = leftResultSet.getMapRowProvider(this, OperationUtils.getPairDecoder(leftResultSet, spliceLeftRuntimeContext),spliceLeftRuntimeContext);
						RowProvider rightProvider = rightResultSet.getMapRowProvider(this, OperationUtils.getPairDecoder(rightResultSet, spliceRightRuntimeContext),spliceRightRuntimeContext);
						return new SpliceNoPutResultSet(activation,this,RowProviders.combine(leftProvider, rightProvider));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
		}

		@Override
		public SpliceOperation getRightOperation() {
				return rightResultSet;
		}

		@Override
		public void open() throws StandardException, IOException {
				super.open();
				leftResultSet.open();
				rightResultSet.open();
		}

        @Override
        public void close() throws StandardException, IOException {
            super.close();
            if (leftResultSet != null) leftResultSet.close();
            if (rightResultSet != null) rightResultSet.close();
        }

		@Override
		public void init(SpliceOperationContext context) throws StandardException, IOException {
				super.init(context);
				leftResultSet.init(context);
				rightResultSet.init(context);
				startExecutionTime = System.currentTimeMillis();
				this.side = context.getRuntimeContext().getPathSide(resultSetNumber);
		}

		@Override
		public List<NodeType> getNodeTypes() {
				return sequentialNodeTypes;
		}

		@Override
		public List<SpliceOperation> getSubOperations() {
				return Arrays.asList(leftResultSet, rightResultSet);
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
								row = leftResultSet.nextRow(spliceRuntimeContext);
								break;
						case RIGHT:
								row = rightResultSet.nextRow(spliceRuntimeContext);
								break;
						case MERGED:
								row = leftResultSet.nextRow(spliceRuntimeContext);
								if(row==null)
										row = rightResultSet.nextRow(spliceRuntimeContext);
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
		public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				if(OperationUtils.isInMemory(this)){
//				if(leftResultSet instanceof RowOperation && rightResultSet instanceof RowOperation){
						spliceRuntimeContext.addPath(resultSetNumber, SpliceRuntimeContext.Side.MERGED);
						return leftResultSet.getMapRowProvider(top,decoder,spliceRuntimeContext);
				}

				SpliceRuntimeContext left = spliceRuntimeContext.copy();
				SpliceRuntimeContext right = spliceRuntimeContext.copy();
				left.addPath(resultSetNumber, SpliceRuntimeContext.Side.LEFT);
				right.addPath(resultSetNumber, SpliceRuntimeContext.Side.RIGHT);
				RowProvider leftRowProvider = leftResultSet.getMapRowProvider(top,decoder,left);
				RowProvider rightRowProvider = rightResultSet.getMapRowProvider(top,decoder,right);
				return RowProviders.combine(leftRowProvider, rightRowProvider);
		}

		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException {
				return getMapRowProvider(top,decoder,spliceRuntimeContext);
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
    public boolean providesRDD() {
        return leftResultSet.providesRDD() && rightResultSet.providesRDD();
    }

    public JavaRDD<LocatedRow> getRDD(SpliceRuntimeContext spliceRuntimeContext, SpliceOperation top) throws StandardException {
        SpliceRuntimeContext left = spliceRuntimeContext.copy();
        SpliceRuntimeContext right = spliceRuntimeContext.copy();
        left.addPath(resultSetNumber, SpliceRuntimeContext.Side.LEFT);
        right.addPath(resultSetNumber, SpliceRuntimeContext.Side.RIGHT);
        JavaRDD<LocatedRow> leftRDD = leftResultSet.getRDD(spliceRuntimeContext, top);
        JavaRDD<LocatedRow> rightRDD = rightResultSet.getRDD(spliceRuntimeContext, top);

        JavaRDD<LocatedRow> result = leftRDD.union(rightRDD);
//        RDDUtils.printRDD("Union result", result);
        return result;
    }

	@Override
	public String getOptimizerOverrides(){
		String left = leftResultSet.getOptimizerOverrides();
		String right = rightResultSet.getOptimizerOverrides();
		if(left!=null){
			if(right!=null) return left+","+right;
			else return left;
		}else if(right!=null) return right;
		else return right;
	}
}
