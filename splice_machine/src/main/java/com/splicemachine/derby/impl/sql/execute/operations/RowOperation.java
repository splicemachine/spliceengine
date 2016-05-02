package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.metrics.Counter;
import com.splicemachine.pipeline.exception.Exceptions;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.spark.api.java.JavaRDD;


public class RowOperation extends SpliceBaseOperation {
		private static final long serialVersionUID = 2l;
		private static Logger LOG = Logger.getLogger(RowOperation.class);
		protected int rowsReturned;
		protected boolean canCacheRow;
		protected boolean next = false;
		protected SpliceMethod<ExecRow> row;
		protected ExecRow cachedRow;
		private ExecRow rowDefinition;
		private String rowMethodName; //name of the row method for
        private boolean rowReturned;

        protected static final String NAME = RowOperation.class.getSimpleName().replaceAll("Operation","");

    	@Override
    	public String getName() {
    			return NAME;
    	}

        
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
				this.canCacheRow = canCacheRow;
				this.rowMethodName = row.getMethodName();
				try {
						init(SpliceOperationContext.newContext(activation));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
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
				try {
						init(SpliceOperationContext.newContext(activation));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
				recordConstructorTime();
		}


		@Override
		public void init(SpliceOperationContext context) throws StandardException, IOException {
				super.init(context);
				if (row == null && rowMethodName != null) {
						if (rowMethodName != null)
								this.row = new SpliceMethod<ExecRow>(rowMethodName, activation);
				}
				startExecutionTime = System.currentTimeMillis();
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

		@Override
		public void	open() throws StandardException  {
				try {
						super.open();
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
				next = false;
		}

		@Override
		public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				SpliceLogUtils.trace(LOG, "nextRow, next=%s, cachedRow=%s",next,cachedRow);

				if(timer==null){
						timer = spliceRuntimeContext.newTimer();
				}
				timer.startTiming();
                currentRow = getRow();
				if(currentRow!=null) {
                    rowReturned = true;
                    timer.tick(1);
                } else {
                    timer.stopTiming();
                    stopExecutionTime = System.currentTimeMillis();
				}
				return currentRow;
		}

		private ExecRow getRow() throws StandardException {
            if (!next) {
                next = true;
                if (cachedRow != null) {
                    SpliceLogUtils.trace(LOG, "getRow,cachedRow=%s", cachedRow);
                    return cachedRow;
                }

                if (row != null) {
                    currentRow = row.invoke();
                    if (canCacheRow) {
                        cachedRow = currentRow;
                    }
                }
            }
            else {
                if (rowReturned) {
                    currentRow = null;
                }
            }
            return currentRow;
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
		public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
				SpliceLogUtils.trace(LOG, "executeScan");
				try {
						return new SpliceNoPutResultSet(activation,this, getMapRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext),runtimeContext));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
		}

		@Override
		public RowProvider getMapRowProvider(SpliceOperation top,PairDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				SpliceLogUtils.trace(LOG, "getMapRowProvider,top=%s",top);
				top.init(SpliceOperationContext.newContext(activation));

				//make sure the runtime context knows it can be merged
				spliceRuntimeContext.addPath(resultSetNumber, SpliceRuntimeContext.Side.MERGED);
				return RowProviders.openedSourceProvider(top, LOG, spliceRuntimeContext);
		}

		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException {
				return getMapRowProvider(top,rowDecoder,spliceRuntimeContext);
		}

		@Override
		public ExecRow getExecRowDefinition() throws StandardException {
            if (rowDefinition == null) {
                ExecRow templateRow = getRow();
                if (templateRow != null) {
                    rowDefinition = templateRow.getClone();
                }
                SpliceLogUtils.trace(LOG,"execRowDefinition=%s",rowDefinition);
            }
			return rowDefinition;
		}

		public int getRowsReturned() {
				return this.rowsReturned;
		}

		@Override
		public void	close() throws StandardException, IOException {
				SpliceLogUtils.trace(LOG,"close in RowOp");
				beginTime = getCurrentTimeMillis();
				super.close();
				next = false;
				closeTime += getElapsedMillis(beginTime);
		}

		@Override
		public long getTimeSpent(int type)
		{
				return constructorTime + openTime + nextTime + closeTime;
		}

		@Override
		public String prettyPrint(int indentLevel) {
				String indent = "\n"+ Strings.repeat("\t",indentLevel);

				return new StringBuilder("RowOp:")
								.append(indent).append("resultSetNumber:").append(resultSetNumber)
								.append(indent).append("rowsReturned:").append(rowsReturned)
								.append(indent).append("canCacheRow:").append(canCacheRow)
								.append(indent).append("rowMethodName:").append(rowMethodName)
								.toString();
		}

		@Override
		public int[] getRootAccessedCols(long tableNumber) {
				return null;
		}

		@Override
		public boolean isReferencingTable(long tableNumber) {
				return false;
		}

		@Override
		public boolean providesRDD() {
			return true;
		}

		@Override
		public JavaRDD<LocatedRow> getRDD(SpliceRuntimeContext spliceRuntimeContext, SpliceOperation top) throws StandardException {
			return SpliceSpark.getContext().parallelize(Lists.newArrayList(new LocatedRow(getRow())), 1);
		}

	@Override
	public String getOptimizerOverrides(SpliceRuntimeContext ctx){
		return null;
	}
}
