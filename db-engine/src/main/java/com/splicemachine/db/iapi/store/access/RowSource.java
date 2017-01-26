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

package com.splicemachine.db.iapi.store.access;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.types.DataValueDescriptor;

import com.splicemachine.db.iapi.services.io.FormatableBitSet;

/**

  A RowSource is the mechanism for iterating over a set of rows.  The RowSource
  is the interface through which access recieved a set of rows from the client
  for the purpose of inserting into a single conglomerate.

  <p>
  A RowSource can come from many sources - from rows that are from fast path
  import, to rows coming out of a sort for index creation.

*/ 
public interface RowSource {

	/**
		Get the next row as an array of column objects. The column objects can
		be a JBMS Storable or any
		Serializable/Externalizable/Formattable/Streaming type.
		<BR>
		A return of null indicates that the complete set of rows has been read.

		<p>
		A null column can be specified by leaving the object null, or indicated
		by returning a non-null getValidColumns.  On streaming columns, it can
		be indicated by returning a non-null get FieldStates.

		<p>
        If RowSource.needToClone() is true then the returned row 
        (the DataValueDescriptor[]) is guaranteed not to be modified by drainer
        of the RowSource (except that the input stream will be read, of course) 
        and drainer will keep no reference to it before making the subsequent 
        nextRow call.  So it is safe to return the same DataValueDescriptor[] 
        in subsequent nextRow calls if that is desirable for performance 
        reasons.  

		<p>
        If RowSource.needToClone() is false then the returned row (the 
        DataValueDescriptor[]) may be be modified by drainer of the RowSource, 
        and the drainer may keep a reference to it after making the subsequent 
        nextRow call.  In this case the client should severe all references to 
        the row after returning it from getNextRowFromRowSource().

		@exception StandardException Standard Derby Error Policy
	 */
	public DataValueDescriptor[] getNextRowFromRowSource() 
        throws StandardException;

	/**
        Does the caller of getNextRowFromRowSource() need to clone the row
        in order to keep a reference to the row past the 
        getNextRowFromRowSource() call which returned the row.  This call
        must always return the same for all rows in a RowSource (ie. the
        caller will call this once per scan from a RowSource and assume the
        behavior is true for all rows in the RowSource).

	 */
	public boolean needsToClone();

	/**
	  getValidColumns describes the DataValueDescriptor[] returned by all calls
      to the getNextRowFromRowSource() call. 

	  If getValidColumns returns null, the number of columns is given by the
	  DataValueDescriptor.length where DataValueDescriptor[] is returned by the
      preceeding getNextRowFromRowSource() call.  Column N maps to 
      DataValueDescriptor[N], where column numbers start at zero.

	  If getValidColumns return a non null validColumns FormatableBitSet the number of
	  columns is given by the number of bits set in validColumns.  Column N is
	  not in the partial row if validColumns.get(N) returns false.  Column N is
	  in the partial row if validColumns.get(N) returns true.  If column N is
	  in the partial row then it maps to DataValueDescriptor[M] where M is the 
      count of calls to validColumns.get(i) that return true where i < N.  If
	  DataValueDescriptor.length is greater than the number of columns 
      indicated by validColumns the extra entries are ignored.  
	*/
	FormatableBitSet getValidColumns(); 

	/**
		closeRowSource tells the RowSource that it will no longer need to
		return any rows and it can release any resource it may have.
		Subsequent call to any method on the RowSource will result in undefined
		behavior.  A closed rowSource can be closed again.
	*/
	void closeRowSource();
}
