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

package com.splicemachine.db.iapi.sql.execute;

/**
 * This is an extension of ExecRow for use
 * with indexes and sorting.
 *
 */
public interface ExecIndexRow extends ExecRow  {

	/**
	 * These two methods are a sort of a hack.  The store implements ordered
	 * null semantics for start and stop positioning, which is correct for
	 * IS NULL and incorrect for everything else.  To work around this,
	 * TableScanResultSet will check whether the start and stop positions
	 * have NULL in any column position other than for an IS NULL check.
	 * If so, it won't do the scan (that is, it will return no rows).
	 *
	 * This method is to inform this ExecIndexRow (which can be used for
	 * start and stop positioning) that the given column uses ordered null
	 * semantics.
	 *
	 * @param columnPosition	The position of the column that uses ordered
	 *							null semantics (zero-based).
	 */
	void orderedNulls(int columnPosition);

	/**
	 * Return true if orderedNulls was called on this ExecIndexRow for
	 * the given column position.
	 *
	 * @param columnPosition	The position of the column (zero-based) for
	 *							which we want to check if ordered null semantics
	 *							are used.
	 *
	 * @return	true if we are to use ordered null semantics on the given column
	 */
	boolean areNullsOrdered(int columnPosition);

	/**
	 * Turn the ExecRow into an ExecIndexRow.
	 */
	void execRowToExecIndexRow(ExecRow valueRow);


}
