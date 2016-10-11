/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;


/**
	Basic implementation of ExecIndexRow.

 */
public class IndexRow extends ValueRow implements ExecIndexRow
{
	///////////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	///////////////////////////////////////////////////////////////////////


	private boolean[]	orderedNulls;

	///////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	///////////////////////////////////////////////////////////////////////

	public IndexRow() {

	}

	public IndexRow(int ncols) {
					this(ncols,new boolean[ncols]);
	}

	public static IndexRow createRaw(int ncols){
					return new IndexRow(ncols,null);
	}

	private IndexRow(int ncols, boolean[] orderedNulls){
					super(ncols);
					this.orderedNulls = orderedNulls;
	}

	///////////////////////////////////////////////////////////////////////
	//
	//	EXECINDEXROW INTERFACE
	//
	///////////////////////////////////////////////////////////////////////

	/* Column positions are one-based, arrays are zero-based */
	public void orderedNulls(int columnPosition) {
		orderedNulls[columnPosition] = true;
	}

	public boolean areNullsOrdered(int columnPosition) {
		return orderedNulls[columnPosition];
	}

	public boolean[] getOrderedNulls(){
					return orderedNulls;
	}

	public void setOrderedNulls(boolean[] orderedNulls){
					this.orderedNulls = orderedNulls;
	}

	/**
	 * Turn the ExecRow into an ExecIndexRow.
	 */
	public void execRowToExecIndexRow(ExecRow valueRow)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"execRowToExecIndexRow() not expected to be called for IndexRow");
		}
	}

	public ExecRow cloneMe() {
		return new IndexRow(nColumns());
	}
}
