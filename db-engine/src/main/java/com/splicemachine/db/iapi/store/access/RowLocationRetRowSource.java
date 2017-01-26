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

import com.splicemachine.db.iapi.types.RowLocation;

/**

  A RowLocationRetRowSource is the mechanism for iterating over a set of rows,
  loading those rows into a conglomerate, and returning the RowLocation of the
  inserted rows. 

  @see RowSource

*/ 
public interface RowLocationRetRowSource extends RowSource 
{

	/**
		needsRowLocation returns true iff this the row source expects the
		drainer of the row source to call rowLocation after getting a row from
		getNextRowFromRowSource.

		@return true iff this row source expects some row location to be
		returned 
		@see #rowLocation
	 */
	boolean needsRowLocation();

	/**
		rowLocation is a callback for the drainer of the row source to return
		the rowLocation of the current row, i.e, the row that is being returned
		by getNextRowFromRowSource.  This interface is for the purpose of
		loading a base table with index.  In that case, the indices can be
		built at the same time the base table is laid down once the row
		location of the base row is known.  This is an example pseudo code on
		how this call is expected to be used:
		
		<BR><pre>
		boolean needsRL = rowSource.needsRowLocation();
		DataValueDescriptor[] row;
		while((row = rowSource.getNextRowFromRowSource()) != null)
		{
			RowLocation rl = heapConglomerate.insertRow(row);
			if (needsRL)
				rowSource.rowLocation(rl);
		}
		</pre><BR>

		NeedsRowLocation and rowLocation will ONLY be called by a drainer of
		the row source which CAN return a row location.  Drainer of row source
		which cannot return rowLocation will guarentee to not call either
		callbacks. Conversely, if NeedsRowLocation is called and it returns
		true, then for every row return by getNextRowFromRowSource, a
		rowLocation callback must also be issued with the row location of the
		row.  Implementor of both the source and the drain of the row source
		must be aware of this protocol.

		<BR>
		The RowLocation object is own by the caller of rowLocation, in other
		words, the drainer of the RowSource.  This is so that we don't need to
		new a row location for every row.  If the Row Source wants to keep the
		row location, it needs to clone it (RowLocation is a ClonableObject).
		@exception StandardException on error
	 */
	void rowLocation(RowLocation rl) throws StandardException;
}
