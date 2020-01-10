/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;
import com.splicemachine.db.iapi.sql.execute.RowChanger;
import com.splicemachine.db.iapi.sql.execute.TargetResultSet;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.GenericResultDescription;
import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.util.Iterator;

/**
 * A simple implementation of a NoPutResultSet that allows for iterating through its rows.  In order to instantiate this ResultSet,
 * only a few things are needed: a list of ExecRows, column descriptors, and an activation.  This class is very useful for taking
 * data that is not stored in the database and presenting it as if it is from the database.  Data from JMX, properties files,
 * Zookeeper, or other sources can be passed into an instance of this class and returned by the database.  It is sort of a poor
 * man's VTI without the relational operations available.  This class was added for Splice Machine.
 *
 * @author Scott Fines
 *         Date: 2/4/14
 */
public class IteratorNoPutResultSet implements NoPutResultSet {
		private final Activation activation;
		private Iterator<ExecRow> nextRow;
        private Iterable<ExecRow> iterable;
		private ExecRow currentRow;
		private final ResultDescription description;

        public IteratorNoPutResultSet(Iterable<ExecRow> iterable, ResultColumnDescriptor[] columnDescriptors,Activation activation) {
            this.iterable = iterable;
            this.activation = activation;
            this.description = new GenericResultDescription(columnDescriptors,"select");
        }


    @Override public void markAsTopResultSet() {  }

		@Override public void openCore() throws StandardException {
            this.nextRow = iterable.iterator();
        }

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

		@Override public double getEstimatedRowCount() { return 0; }

		@Override public int resultSetNumber() { return 0; }

		@Override public void setCurrentRow(ExecRow row) { this.currentRow = row;	 }

		@Override public boolean requiresRelocking() { return false; }

		@Override public boolean isForUpdate() { return false; }

		@Override public void updateRow(ExecRow row, RowChanger rowChanger) throws StandardException {  }

		@Override public void markRowAsDeleted() throws StandardException {  }

		@Override public void positionScanAtRowLocation(RowLocation rLoc) throws StandardException {  }

		@Override public boolean returnsRows() { return true; }

		@Override public long[] modifiedRowCount() { return new long[]{0}; }

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
			    throw new RuntimeException("Not Supported");
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
				throw new RuntimeException("Not Supported");
		}

		@Override
		public ExecRow getFirstRow() throws StandardException {
            throw new RuntimeException("Not Supported");
		}

		@Override public ExecRow getNextRow() throws StandardException { return getNextRowCore(); }

		@Override public ExecRow getPreviousRow() throws StandardException { return null; }

		@Override
		public ExecRow getLastRow() throws StandardException {
            throw new RuntimeException("Not Supported");
		}

		@Override
		public ExecRow setAfterLastRow() throws StandardException {
            throw new RuntimeException("Not Supported");
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

	@Override
	public boolean isKilled() {
		return false;
	}

	@Override
	public boolean isTimedout() {
		return false;
	}

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
