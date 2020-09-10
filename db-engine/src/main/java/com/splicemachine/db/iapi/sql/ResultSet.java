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

package com.splicemachine.db.iapi.sql;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;

import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.util.UUID;

/**
 * The ResultSet interface provides a method to tell whether a statement
 * returns rows, and if so, a method to get the rows. It also provides a
 * method to get metadata about the contents of the rows. It also provide
 * a method to accept rows as input.
 * <p>
 * There is no single implementation of the ResultSet interface. Instead,
 * the various support operations involved in executing statements
 * implement this interface.
 * <p>
 * Although ExecRow is used on the interface, it is not available to
 * users of the API. They should use Row, the exposed super-interface
 * of ExecRow.  <<I couldn't find another way to perform this mapping...>>
 * <p>
 * Valid transitions: <ul>
 * <li> open->close</li>
 * <li> close->open</li>
 * <li> close->finished</li>
 * <li> finished->open</li>
 * </ul>
 *
 */

public interface ResultSet
{
	/* Get time only spent in this ResultSet */
	int CURRENT_RESULTSET_ONLY = 0;
	/* Get time spent in this ResultSet and below */
	int ENTIRE_RESULTSET_TREE = 1;

	// cursor check positioning
	int ISBEFOREFIRST = 101;
	int ISFIRST = 102;
	int ISLAST = 103;
	int ISAFTERLAST = 104;

	/**
	 * Returns TRUE if the statement returns rows (i.e. is a SELECT
	 * or FETCH statement), FALSE if it returns no rows.
	 *
	 * @return	TRUE if the statement returns rows, FALSE if not.
	 */
	 boolean	returnsRows();

	/**
	 * Returns the number of rows affected by the statement.
	   Only valid of returnsRows() returns false.
	 * For other DML statements, it returns the number of rows
	 * modified by the statement. For statements that do not affect rows
	 * (like DDL statements), it returns zero.
	 *
	 * @return	The number of rows affect by the statement, so far.
	 */
	long[]	modifiedRowCount();

	/**
	 * Returns a ResultDescription object, which describes the results
	 * of the statement this ResultSet is in. This will *not* be a
	 * description of this particular ResultSet, if this is not the
	 * outermost ResultSet.
	 *
	 * @return	A ResultDescription describing the results of the
	 *		statement.
	 */
	ResultDescription	getResultDescription();
	
	Activation getActivation();

	/**
	 * Needs to be called before the result set will do anything.
	 * Need to call before getNextRow(), or for a result set
	 * that doesn't return rows, this is the call that will
	 * cause all the work to be done.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	void	open() throws StandardException;

	/**
	 * Returns the row at the absolute position from the query, 
	 * and returns NULL when there is no such position.
	 * (Negative position means from the end of the result set.)
	 * Moving the cursor to an invalid position leaves the cursor
	 * positioned either before the first row (negative position)
	 * or after the last row (positive position).
	 * NOTE: An exception will be thrown on 0.
	 *
	 * @param row	The position.
	 * @return	The row at the absolute position, or NULL if no such position.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	ExecRow	getAbsoluteRow(int row) throws StandardException;

	/**
	 * Returns the row at the relative position from the current
	 * cursor position, and returns NULL when there is no such position.
	 * (Negative position means toward the beginning of the result set.)
	 * Moving the cursor to an invalid position leaves the cursor
	 * positioned either before the first row (negative position)
	 * or after the last row (positive position).
	 * NOTE: 0 is valid.
	 * NOTE: An exception is thrown if the cursor is not currently
	 * positioned on a row.
	 *
	 * @param row	The position.
	 * @return	The row at the relative position, or NULL if no such position.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	ExecRow	getRelativeRow(int row) throws StandardException;

	/**
	 * Sets the current position to before the first row and returns NULL
	 * because there is no current row.
	 *
	 * @return	NULL.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	ExecRow	setBeforeFirstRow() throws StandardException;

	/**
	 * Returns the first row from the query, and returns NULL when there
	 * are no rows.
	 *
	 * @return	The first row, or NULL if no rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	ExecRow	getFirstRow() throws StandardException;

	/**
	 * Returns the next row from the query, and returns NULL when there
	 * are no more rows.
	 *
	 * @return	The next row, or NULL if no more rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	ExecRow	getNextRow() throws StandardException;

	/**
	 * Returns the previous row from the query, and returns NULL when there
	 * are no more previous rows.
	 *
	 * @return	The previous row, or NULL if no more previous rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	ExecRow	getPreviousRow() throws StandardException;

	/**
	 * Returns the last row from the query, and returns NULL when there
	 * are no rows.
	 *
	 * @return	The last row, or NULL if no rows.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	ExecRow	getLastRow() throws StandardException;

	/**
	 * Sets the current position to after the last row and returns NULL
	 * because there is no current row.
	 *
	 * @return	NULL.
	 *
	 * @exception StandardException		Thrown on failure
	 * @see Row
	 */
	ExecRow	setAfterLastRow() throws StandardException;

	/**
	 * Clear the current row. The cursor keeps it current position,
	 * however it cannot be used for positioned updates or deletes
	 * until a fetch is done.
	 * This is done after a commit on holdable
	 * result sets.
	 * A fetch is achieved by calling one of the positioning 
	 * methods: getLastRow(), getNextRow(), getPreviousRow(), 
	 * getFirstRow(), getRelativeRow(..) or getAbsoluteRow(..).
	 */
	void clearCurrentRow();
	 
    /**
		Determine if the result set is at one of the positions
		according to the constants above (ISBEFOREFIRST etc).
		Only valid and called for scrollable cursors.
     * @return true if at the requested position.
	 * @exception StandardException Thrown on error.
     */
	boolean checkRowPosition(int isType) throws StandardException;

	/**
	 * Returns the row number of the current row.  Row
	 * numbers start from 1 and go to 'n'.  Corresponds
	 * to row numbering used to position current row
	 * in the result set (as per JDBC).

		Only valid and called for scrollable cursors.
	 * @return	the row number, or 0 if not on a row
	 *
	 */
	int	getRowNumber();

	/**
	 * Tells the system that there will be no more calls to getNextRow()
	 * (until the next open() call), so it can free up the resources
	 * associated with the ResultSet.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	void	close() throws StandardException;

	/**
	 * Tells the system to clean up on an error.
	 *
	 * @exception StandardException		Thrown on error.
	 */
	void	cleanUp() throws StandardException;

	/**
		Find out if the ResultSet is closed or not.
		Will report true for result sets that do not return rows.

		@return true if the ResultSet has been closed.
	 */
	boolean isClosed();

	/**
	 Find out if the ResultSet is killed or not.

	 @return true if the ResultSet has been killed.
	 */
	boolean isKilled();

	/**
	 * Tells the system that there will be no more access
	 * to any database information via this result set;
	 * in particular, no more calls to open().
	 * Will close the result set if it is not already closed.
	 *
	 * @exception StandardException	on error
	 */
	void	finish() throws StandardException;

	/**
	 * Get the execution time in milliseconds.
	 *
	 * @return long		The execution time in milliseconds.
	 */
	long getExecuteTime();

	/**
	 * Get the Timestamp for the beginning of execution.
	 *
	 * @return Timestamp		The Timestamp for the beginning of execution.
	 */
	Timestamp getBeginExecutionTimestamp();

	/**
	 * Get the Timestamp for the end of execution.
	 *
	 * @return Timestamp		The Timestamp for the end of execution.
	 */
	Timestamp getEndExecutionTimestamp();

	/**
	 * Return the total amount of time spent in this ResultSet
	 *
	 * @param type	CURRENT_RESULTSET_ONLY - time spent only in this ResultSet
	 *				ENTIRE_RESULTSET_TREE  - time spent in this ResultSet and below.
	 *
	 * @return long		The total amount of time spent (in milliseconds).
	 */
	long getTimeSpent(int type);

	/**
	 * Get the subquery ResultSet tracking array from the top ResultSet.
	 * (Used for tracking open subqueries when closing down on an error.)
	 *
	 * @param numSubqueries		The size of the array (For allocation on demand.)
	 *
	 * @return NoPutResultSet[]	Array of NoPutResultSets for subqueries.
	 */
	NoPutResultSet[] getSubqueryTrackingArray(int numSubqueries);

	/**
	 * ResultSet for rows inserted into the table (contains auto-generated keys columns only)
	 *
	 * @return NoPutResultSet	NoPutResultSets for rows inserted into the table.
	 */
	ResultSet getAutoGeneratedKeysResultset();

	/**
	 * Returns the name of the cursor, if this is cursor statement of some
	 * type (declare, open, fetch, positioned update, positioned delete,
	 * close).
	 *
	 * @return	A String with the name of the cursor, if any. Returns
	 *		NULL if this is not a cursor statement.
	 */
	String	getCursorName();

    /**
     * Add a warning to this result set.
     *
     * @param w the warning to add
     */
    void addWarning(SQLWarning w);

	/**
		Return the set of warnings generated during the execution of
		this result set. The warnings are cleared once this call returns.
	*/
	SQLWarning getWarnings();

	/**
	 Find out if the ResultSet is timed out or not.

	 @return true if the ResultSet has timed out.
	 */
    boolean isTimedout();

	/**
	 * Get the UUID of the underlying operation.
	 *
	 * @return UUID of the underlying operation.
	 */
	default UUID getUuid() { return null; }
}
