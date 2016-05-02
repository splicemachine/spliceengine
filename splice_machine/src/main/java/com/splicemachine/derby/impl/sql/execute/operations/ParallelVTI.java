package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.vti.VTITemplate;
import org.apache.log4j.Logger;

import java.io.*;
import java.sql.*;
import java.util.Collections;
import java.util.List;

/**
 *
 * Base class for VTIs which are computed in parallel.
 *
 * <p>Implementations of this base class will initiate a call to the SpliceEngine
 * to perform work in parallel. When using this, the task will be broken up into N computation blocks
 * (where N is the number of regions specified by getSplits()); Each computation block will execute sequentially,
 * in a map-only MR job style. Thus, this class should <em>only</em> be implemented when the desired task
 * can be accomplished <em>entirely</em> in parallel. For example, Data imports will work will, but multi-stage
 * aggregation will not.
 *
 * @author Scott Fines
 */
public abstract class ParallelVTI extends VTITemplate implements SpliceOperation,Externalizable {
		private static final Logger LOG = Logger.getLogger(ParallelVTI.class);

		private static final List<NodeType> nodeTypes = Collections.singletonList(NodeType.REDUCE);

		private Activation activation;
		private boolean isTopResultSet;

		private double optimizerEstimatedRowCount;
		private int resultSetNumber;
		private ExecRow currentRow;

		private byte[] uniqueSequenceId;

		@Override
		public void init(SpliceOperationContext context){
				this.activation = context.getActivation();
		}



		@Override
		public SQLWarning getWarnings() {
				try {
						return super.getWarnings();
				} catch (SQLException e) {
						SpliceLogUtils.logAndThrowRuntime(LOG, e);
						return null;
				}
		}

		@Override
		public OperationRuntimeStats getMetrics(long statementId, long taskId, boolean isTopOperation) {
				throw new UnsupportedOperationException("Statistics are not implemented for "+ this.getClass());
		}

		@Override
		public String getCursorName()  {
				try {
						return super.getCursorName();
				} catch (SQLException e) {
						SpliceLogUtils.logAndThrowRuntime(LOG,e);
						return null;
				}
		}



		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				throw new UnsupportedOperationException();

		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,
						ClassNotFoundException {
				throw new UnsupportedOperationException();
		}

		@Override public void markAsTopResultSet() { this.isTopResultSet=true; }

		public ExecRow getNextSinkRow() throws StandardException {
				throw new UnsupportedOperationException();
		}

		@Override
		public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				throw new UnsupportedOperationException();
		}

	/*default implementations and no-ops*/

		public double getEstimatedRowCount() { return this.optimizerEstimatedRowCount; }

	@Override
	public String getOptimizerOverrides(SpliceRuntimeContext ctx){
		return null;
	}

	@Override
	public int getQueryNiceness(SpliceRuntimeContext ctx){
		return -1;
	}

	@Override public int resultSetNumber() { return this.resultSetNumber; }
		@Override public Activation getActivation() { return activation; }

		@Override
		public void setCurrentRow(ExecRow row) {
				activation.setCurrentRow(row, resultSetNumber);
				currentRow = row;
		}
	

	/*Default UnsupportedOperations*/



		/*Actual SpliceBaseOperations to be implemented*/
		@Override
		public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder,SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				throw new UnsupportedOperationException();
		}

		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException {
				throw new UnsupportedOperationException();
		}

		@Override
		public final List<NodeType> getNodeTypes() {
				return nodeTypes;
		}

		@Override
		public List<SpliceOperation> getSubOperations() {
				throw new UnsupportedOperationException();
		}

		@Override
		public byte[] getUniqueSequenceID() {
				return uniqueSequenceId;
		}

		@Override
		public abstract void executeShuffle(SpliceRuntimeContext runtimeContext) throws StandardException;

		@Override
		public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
				throw new UnsupportedOperationException("Scans are not supported in Fully parallel mode");
		}

		@Override
		public SpliceNoPutResultSet executeProbeScan() throws StandardException {
				throw new UnsupportedOperationException("Scans are not supported in Fully parallel mode");
		}

		@Override
		public SpliceOperation getLeftOperation() {
				throw new UnsupportedOperationException();
		}

		@Override
		public final void generateLeftOperationStack(List<SpliceOperation> operations) {
				SpliceLogUtils.trace(LOG, "generateLeftOperationStack");
				OperationUtils.generateLeftOperationStack(this, operations);
		}

		@Override
		public abstract ExecRow getExecRowDefinition();

		@Override
		public abstract boolean next();

		@Override
		public abstract void close();


		@Override
		public SpliceOperation getRightOperation() {
				return null;  //To change body of implemented methods use File | Settings | File Templates.
		}

		@Override
		public void generateRightOperationStack(boolean initial, List<SpliceOperation> operations) {
				//To change body of implemented methods use File | Settings | File Templates.
		}

		//    @Override
		public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
				throw new UnsupportedOperationException();
		}

		//    @Override
		public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
				throw new UnsupportedOperationException();
		}

		@Override
		public RowLocation getCurrentRowLocation() {
				throw new UnsupportedOperationException();
		}

        public int[] getAccessedNonPkColumns() {
            return null;
        }


		@Override
		public void setActivation(Activation activation) {
			throw new UnsupportedOperationException();			
		}
		
		
}
