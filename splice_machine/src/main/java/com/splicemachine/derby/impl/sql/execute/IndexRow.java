

/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;


/**
	Basic implementation of ExecIndexRow.

 */
@SuppressFBWarnings(value = "EQ_DOESNT_OVERRIDE_EQUALS",justification = "Intentional")
public class IndexRow extends ValueRow implements ExecIndexRow {
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


    public IndexRow(){ }

    public IndexRow(int ncols) {
		 super(ncols);
		 orderedNulls = new boolean[ncols];	/* Initializes elements to false */
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