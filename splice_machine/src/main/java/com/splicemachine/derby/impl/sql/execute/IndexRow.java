

package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;


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