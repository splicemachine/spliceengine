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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.sql;

/**
 * The ResultDescription interface provides methods to get metadata on the
 * results returned by a statement.
 *
 */

public interface ResultDescription
{
	/**
	 * Returns an identifier that tells what type of statement has been
	 * executed. This can be used to determine what other methods to call
	 * to get the results back from a statement. For example, a SELECT
	 * statement returns rows and columns, while other statements don't,
	 * so you would only call getColumnCount() or getColumnType() for
	 * SELECT statements.
	 *
	 * @return	A String identifier telling what type of statement this
	 *		is.
	 */
	String	getStatementType();	

	/**
	 * Returns the number of columns in the result set.
	 *
	 * @return	The number of columns in the result set.
	 */
	int	getColumnCount();

	/**
		Return information about all the columns.
	*/
	public ResultColumnDescriptor[] getColumnInfo();

	/**
	 * Returns a ResultColumnDescriptor for the column, given the ordiinal
	 * position of the column.
	 * NOTE - position is 1-based.
	 *
	 * @param position	The oridinal position of a column in the
	 *			ResultSet.
	 *
	 * @return		A ResultColumnDescriptor describing the
	 *			column in the ResultSet.
	 */
	ResultColumnDescriptor	getColumnDescriptor(int position);

	/**
	 * Get a new result description that has been truncated
	 * from input column number.   If the input column is
	 * 5, then columns 5 to getColumnCount() are removed.
	 * The new ResultDescription points to the same
	 * ColumnDescriptors (this method performs a shallow
	 * copy. The saved JDBC ResultSetMetaData will
     * not be copied.
	 *
	 * @param truncateFrom the starting column to remove,
	 * 1-based.
	 *
	 * @return a new ResultDescription
	 */
	public ResultDescription truncateColumns(int truncateFrom);
    
    /**
     * Set the JDBC ResultSetMetaData for this ResultDescription.
     * A ResultSetMetaData object can be saved in the statement
     * plan using this method. This only works while
     * the ResultSetMetaData api does not contain a getConnection()
     * method or a close method.
     * <BR>
     * If this object already has a saved meta data object
     * this call will do nothing.
     * Due to synchronization the saved ResultSetMetaData
     * object may not be the one passed in, ie. if two
     * threads call this concurrently, only one will be saved.
     * It is assumed the JDBC layer passes in a ResultSetMetaData
     * object based upon this.
     */
    public void setMetaData(java.sql.ResultSetMetaData rsmd);
    
    /**
     * Get the saved JDBC ResultSetMetaData. Will return
     * null if setMetaData() has not been called on this
     * object. The caller then should manufacture a
     * ResultSetMetaData object and pass it into setMetaData.
     */
    public java.sql.ResultSetMetaData getMetaData();
    
    /**
     * Return the position of the column matching the
     * passed in names following the JDBC rules for
     * ResultSet.getXXX and updateXXX.
     * Rules are the matching is case insensitive
     * and the insensitive name matches the first
     * column found that matches (starting at postion 1).
     * @param name
     * @return Position of the column (1-based), -1 if no match.
     */
    public int findColumnInsenstive(String name);
}
