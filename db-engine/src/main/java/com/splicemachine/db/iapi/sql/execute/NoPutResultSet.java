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

import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.iapi.store.access.RowLocationRetRowSource;

/**
 * The NoPutResultSet interface is used to provide additional
 * operations on result sets that can be used in returning rows
 * up a ResultSet tree.
 * <p>
 * Since the ResulSet operations must also be supported by
 * NoPutResultSets, we extend that interface here as well.
 *
 */
public interface NoPutResultSet extends ResultSet, RowLocationRetRowSource 
{
	// method names for use with SQLState.LANG_RESULT_SET_NOT_OPEN exception

	String	ABSOLUTE		=	"absolute";
	String	RELATIVE		=	"relative";
	String	FIRST			=	"first";
	String	NEXT			=	"next";
	String	LAST			=	"last";
	String	PREVIOUS		=	"previous";

	/**
	 * Mark the ResultSet as the topmost one in the ResultSet tree.
	 * Useful for closing down the ResultSet on an error.
	 */
	void markAsTopResultSet();

	/**
	 * open a scan on the table. scan parameters are evaluated
	 * at each open, so there is probably some way of altering
	 * their values...
	 * <p>
	 * openCore() can only be called on a closed result
	 * set.  see reopenCore if you want to reuse an open
	 * result set.
	 * <p>
	 * For NoPutResultSet open() must only be called on
	 * the top ResultSet. Opening of NoPutResultSet's
	 * below the top result set are implemented by calling
	 * openCore.
	 *
	 * @exception StandardException thrown if cursor finished.
	 */
	void openCore() throws StandardException;

	/**
     * reopen the scan.  behaves like openCore() but is 
	 * optimized where appropriate (e.g. where scanController
	 * has special logic for us).  
	 * <p>
	 * used by joiners
	 * <p>
	 * scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...  
	 *
	 * @exception StandardException thrown if cursor finished.
     */
	void reopenCore() throws StandardException;

	/**
     * Return the requested values computed
     * from the next row (if any) for which
     * the restriction evaluates to true.
     * <p>
     * restriction and projection parameters
     * are evaluated for each row.
	 *
	 * @exception StandardException thrown on failure.
	 *
	 * @return the next row in the result
	 */
	ExecRow	getNextRowCore() throws StandardException;

	/**
	 * Return the point of attachment for this subquery.
	 * (Only meaningful for Any and Once ResultSets, which can and will only
	 * be at the top of a ResultSet for a subquery.)
	 *
	 * @return int	Point of attachment (result set number) for this
	 *			    subquery.  (-1 if not a subquery - also Sanity violation)
	 */
	int getPointOfAttachment();

	/**
	 * Return the isolation level of the scan in the result set.
	 * Only expected to be called for those ResultSets that
	 * contain a scan.
	 *
	 * @return The isolation level of the scan (in TransactionController constants).
	 */
	int getScanIsolationLevel();

	/**
	 * Notify a NPRS that it is the source for the specified 
	 * TargetResultSet.  This is useful when doing bulk insert.
	 *
	 * @param trs	The TargetResultSet.
	 */
	void setTargetResultSet(TargetResultSet trs);

	/**
	 * Set whether or not the NPRS need the row location when acting
	 * as a row source.  (The target result set determines this.)
	 */
	void setNeedsRowLocation(boolean needsRowLocation);

	/**
	 * Get the estimated row count from this result set.
	 *
	 * @return	The estimated row count (as a double) from this result set.
	 */
	double getEstimatedRowCount();

	/**
	 * Get the number of this ResultSet, which is guaranteed to be unique
	 * within a statement.
	 */
	int resultSetNumber();

	/**
	 * Set the current row to the row passed in.
	 *
	 * @param row the new current row
	 *
	 */
	void setCurrentRow(ExecRow row);

	/**
	 * Do we need to relock the row when going to the heap.
	 *
	 * @return Whether or not we need to relock the row when going to the heap.
	 */

	boolean requiresRelocking();
	
	/**
	 * Is this ResultSet or it's source result set for update
	 *
	 * @return Whether or not the result set is for update.
	 */
	boolean isForUpdate();

	/* 
	 * New methods for supporting detectability of own changes for
	 * for updates and deletes when using ResultSets of type 
	 * TYPE_SCROLL_INSENSITIVE and concurrency CONCUR_UPDATABLE.
	 */
	
	/**
	 * Updates the resultSet's current row with it's new values after
	 * an update has been issued either using positioned update or
	 * JDBC's udpateRow method.
	 *
	 * @param row new values for the currentRow
	 * @param rowChanger holds information about row: what columns of it is to
	 *        be used for updating, and what underlying base table column each
	 *        such column corresponds to.
	 *
	 * @exception StandardException thrown on failure.
	 */
	void updateRow(ExecRow row, RowChanger rowChanger)
			throws StandardException;
	
	/**
	 * Marks the resultSet's currentRow as deleted after a delete has been 
	 * issued by either by using positioned delete or JDBC's deleteRow
	 * method.
	 *
	 * @exception StandardException thrown on failure.
	 */
	void markRowAsDeleted() throws StandardException;

	/**
	 * Positions the cursor in the specified rowLocation. Used for
	 * scrollable insensitive result sets in order to position the
	 * cursor back to a row that has already be visited.
	 * 
	 * @param rLoc row location of the current cursor row
	 *
	 * @exception StandardException thrown on failure to
	 *	get location from storage engine
	 *
	 */
	void positionScanAtRowLocation(RowLocation rLoc) 
		throws StandardException;}
