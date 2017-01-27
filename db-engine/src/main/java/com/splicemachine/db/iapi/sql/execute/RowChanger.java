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

package com.splicemachine.db.iapi.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.types.RowLocation;

/**
  Perform row at a time DML operations of tables and maintain indexes.
  */
public interface RowChanger
{
	/**
	  Open this RowChanger.

	  <P>Note to avoid the cost of fixing indexes that do not
	  change during update operations use openForUpdate(). 
	  @param lockMode	The lock mode to use
							(row or table, see TransactionController)

	  @exception StandardException thrown on failure to convert
	  */
	public void open(int lockMode)
		 throws StandardException;

	/**
	 * Set the row holder for this changer to use.
	 * If the row holder is set, it wont bother 
	 * saving copies of rows needed for deferred
	 * processing.  Also, it will never close the
	 * passed in rowHolder.
	 *
	 * @param rowHolder	the row holder
	 */
	public void setRowHolder(TemporaryRowHolder rowHolder);

	/**
	 * Sets the index names of the tables indices. Used for error reporting.
	 * 
	 * @param indexNames		Names of all the indices on this table.
	 */
	public void setIndexNames(String[] indexNames);

	/**
	  Open this RowChanger to avoid fixing indexes that do not change
	  during update operations. 

	  @param fixOnUpdate fixOnUpdat[ix] == true ==> fix index 'ix' on
	  an update operation.
	  @param lockMode	The lock mode to use
							(row or table, see TransactionController)
	  @param wait		If true, then the caller wants to wait for locks. False will be
							when we using a nested user xaction - we want to timeout right away
							if the parent holds the lock.  (bug 4821)

	  @exception StandardException thrown on failure to convert
	  */
	public void openForUpdate( boolean[] fixOnUpdate, int lockMode, boolean wait )
		 throws StandardException;

	/**
	  Insert a row into the table and perform associated index maintenance.

	  @param baseRow the row.
	  @exception StandardException		Thrown on error
	  */
	public void insertRow(ExecRow baseRow)
		 throws StandardException;
		
	/**
	  Delete a row from the table and perform associated index maintenance.

	  @param baseRow the row.
	  @param baseRowLocation the row's base conglomerate
	     location
	  @exception StandardException		Thrown on error
	  */
	public void deleteRow(ExecRow baseRow, RowLocation baseRowLocation)
		 throws StandardException;

	/**
	  Update a row in the table and perform associated index maintenance.

	  @param oldBaseRow the old image of the row.
	  @param newBaseRow the new image of the row.
	  @param baseRowLocation the row's base conglomerate
	     location
	  @exception StandardException		Thrown on error
	  */
	public void updateRow(ExecRow oldBaseRow,
						  ExecRow newBaseRow,
						  RowLocation baseRowLocation)
		 throws StandardException;

	/**
	  Finish processing the changes.  This means applying the deferred
	  inserts for updates to unique indexes.

	  @exception StandardException		Thrown on error
	 */
	public void finish()
		throws StandardException;

	/**
	  Close this RowChanger.

	  @exception StandardException		Thrown on error
	  */
	public void close()
		throws StandardException;

	/** 
	 * Return the ConglomerateController from this RowChanger.
	 * This is useful when copying properties from heap to 
	 * temp conglomerate on insert/update/delete.
	 *
	 * @return The ConglomerateController from this RowChanger.
	 */
	public ConglomerateController getHeapConglomerateController();

	/**
	  Open this RowChanger.

	  <P>Note to avoid the cost of fixing indexes that do not
	  change during update operations use openForUpdate(). 
	  @param lockMode	The lock mode to use
							(row or table, see TransactionController)
	  @param wait		If true, then the caller wants to wait for locks. False will be
							when we using a nested user xaction - we want to timeout right away
							if the parent holds the lock.  

	  @exception StandardException thrown on failure to convert
	  */
	public void open(int lockMode, boolean wait)
		 throws StandardException;

	/**
	 * Return what column no in the input ExecRow (cf nextBaseRow argument to
	 * #updateRow) would correspond to selected column, if any.
	 *
	 * @param selectedCol the column number in the base table of a selected
	 *                    column or -1 (if selected column is not a base table
	 *                    column, e.g. i+4).
	 * @return column no, or -1 if not found or not a base column
	 */
	public int findSelectedCol(int selectedCol);
}
