package com.splicemachine.derby.utils;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.RowChanger;
import org.apache.derby.iapi.sql.execute.TargetResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.GenericResultDescription;

import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 2/4/14
 */
public class IteratorNoPutResultSet implements NoPutResultSet {
		private final List<ExecRow> results;
		private final Activation activation;

		private Iterator<ExecRow> nextRow;
		private ExecRow currentRow;

		private final ResultDescription description;


		public IteratorNoPutResultSet(List<ExecRow> results, ResultColumnDescriptor[] columnDescriptors,Activation activation) {
				this.results = results;
				this.activation = activation;

				this.description = new GenericResultDescription(columnDescriptors,"select");
		}

		@Override public void markAsTopResultSet() {  }

		@Override public void openCore() throws StandardException { this.nextRow = results.iterator(); }

		@Override public void reopenCore() throws StandardException { openCore(); }

		@Override
		public ExecRow getNextRowCore() throws StandardException {
				if(!nextRow.hasNext()) return null;
				return nextRow.next();
		}

		@Override public int getPointOfAttachment() { return 0; }

		@Override public int getScanIsolationLevel() { return 0; }

		@Override public void setTargetResultSet(TargetResultSet trs) {  }

		@Override public void setNeedsRowLocation(boolean needsRowLocation) {  }

		@Override public double getEstimatedRowCount() { return results.size(); }

		@Override public int resultSetNumber() { return 0; }

		@Override public void setCurrentRow(ExecRow row) { this.currentRow = row;	 }

		@Override public boolean requiresRelocking() { return false; }

		@Override public boolean isForUpdate() { return false; }

		@Override public void updateRow(ExecRow row, RowChanger rowChanger) throws StandardException {  }

		@Override public void markRowAsDeleted() throws StandardException {  }

		@Override public void positionScanAtRowLocation(RowLocation rLoc) throws StandardException {  }

		@Override public boolean returnsRows() { return true; }

		@Override public int modifiedRowCount() { return 0; }

		@Override
		public ResultDescription getResultDescription() {
				return description;
		}

		@Override
		public Activation getActivation() {
				return activation;
		}

		@Override public void open() throws StandardException { openCore(); }

		@Override
		public ExecRow getAbsoluteRow(int row) throws StandardException {
				return results.get(row);
		}

		@Override
		public ExecRow getRelativeRow(int row) throws StandardException {
				ExecRow nextRowToReturn = null;
				for(int i=0;i<=row;i++){
						if(!nextRow.hasNext()) return null;
						nextRowToReturn = nextRow.next();
				}
				return nextRowToReturn;
		}

		@Override
		public ExecRow setBeforeFirstRow() throws StandardException {
				this.nextRow = results.iterator();
				return results.get(0);
		}

		@Override
		public ExecRow getFirstRow() throws StandardException {
				return results.get(0);
		}

		@Override public ExecRow getNextRow() throws StandardException { return getNextRowCore(); }

		@Override public ExecRow getPreviousRow() throws StandardException { return null; }

		@Override
		public ExecRow getLastRow() throws StandardException {
				return results.get(results.size()-1);
		}

		@Override
		public ExecRow setAfterLastRow() throws StandardException {
				nextRow=null;
				return results.get(results.size()-1);
		}

		@Override public void clearCurrentRow() { this.currentRow=null; }

		@Override
		public boolean checkRowPosition(int isType) throws StandardException {
				return false;
		}

		@Override public int getRowNumber() { return 0; }

		@Override public void close() throws StandardException { nextRow = null; }

		@Override public void cleanUp() throws StandardException {  }

		@Override public boolean isClosed() { return nextRow ==null; }

		@Override public void finish() throws StandardException { close(); }

		@Override public long getExecuteTime() { return 0; }

		@Override public Timestamp getBeginExecutionTimestamp() { return null; }

		@Override public Timestamp getEndExecutionTimestamp() { return null; }

		@Override public long getTimeSpent(int type) { return 0; }

		@Override public NoPutResultSet[] getSubqueryTrackingArray(int numSubqueries) { return new NoPutResultSet[0]; }

		@Override public ResultSet getAutoGeneratedKeysResultset() { return null; }

		@Override public String getCursorName() { return null; }

		@Override public void addWarning(SQLWarning w) {  }

		@Override public SQLWarning getWarnings() { return null; }

		@Override public boolean needsRowLocation() { return false; }

		@Override public void rowLocation(RowLocation rl) throws StandardException {  }

		@Override public DataValueDescriptor[] getNextRowFromRowSource() throws StandardException { return new DataValueDescriptor[0]; }

		@Override public boolean needsToClone() { return false; }

		@Override public FormatableBitSet getValidColumns() { return null; }
		@Override public void closeRowSource() {  }
}
