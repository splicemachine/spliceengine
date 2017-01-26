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

package com.splicemachine.derby.impl.sql.execute.operations.iapi;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.uuid.UUIDGenerator;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;

/**
 * @author Scott Fines
 * Created on: 10/1/13
 */
public interface OperationInformation {

    void initialize(SpliceOperationContext operationContext) throws StandardException;

		public double getEstimatedRowCount();

        public double getEstimatedCost();

        public int getResultSetNumber();

		/**
		 * Return the map from the physical (encoded) column locations to the decoded column locations.
		 *
		 * For example, let's say you are looking at the row (a,b,c,d), and you want to return (a,c,d). In this case,
		 *
		 * 1. the physical location of a is 0,so {@code baseColumnMap[0]} is the logical position of a in
		 * the decoded row. Since the logical location of a in the decoded row is 0, {@code baseColumnMap[0] = 0}
		 * 2. The physical location of b is 1, so {@code baseColumnMap[1]} is the logical position of b in
		 * the decoded row. Since b is to be ignored, the logical position of b is -1. Thus {@code baseColumnMap[1] = -1}
		 * 3. The physical location of c is 2, so {@code baseColumnMap[2] } is the location of c in the decoded row.
		 * Since c is logically placed in position 1, {@code baseColumnMap[2] = 1}
		 * 4. The physical location of d is 3, so {@code baseColumnMap[3]} is the location of d in the decoded row.
		 * Since d is logically placed in position 2 in the decoded row, {@code baseColumnMap[3] = 2}.
		 *
		 * Thus, in this example, {@code baseColumnMap = [0,-1,1,2]}
		 *
		 * @return the map from physical location to logical location for columns in the row.
		 */
        public int[] getBaseColumnMap();

        public ExecRow compactRow(ExecRow candidateRow,
                                  FormatableBitSet accessedColumns,
                                  boolean isKeyed) throws StandardException;

        public ExecRow compactRow(ExecRow candidateRow,
                                  ScanInformation scanInfo) throws StandardException;

        public ExecRow getKeyTemplate(ExecRow candidateRow,
                                  ScanInformation scanInfo) throws StandardException;

        public NoPutResultSet[] getSubqueryTrackingArray() throws StandardException;

        void setCurrentRow(ExecRow row);

        UUIDGenerator getUUIDGenerator();

        ExecutionFactory getExecutionFactory();
}
