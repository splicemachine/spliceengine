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

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.i18n.MessageService;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;
import com.splicemachine.db.iapi.sql.execute.TargetResultSet;
import com.splicemachine.db.iapi.sql.execute.RowChanger;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.store.access.RowLocationRetRowSource;
import com.splicemachine.db.iapi.store.access.RowSource;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.Orderable;
import com.splicemachine.db.iapi.types.RowLocation;


/**
 * Abstract ResultSet with built in Activation support for operations that
 * return rows but do not allow the caller to put data on output pipes. This
 * implementation of ResultSet is meant to be overridden by subtypes in the
 * execution engine. Its primary users will be DML operations that do not put
 * data on output pipes, but simply return it due to being result sets
 * themselves.
 * <p>
 * This abstract class does not define the entire ResultSet
 * interface, but leaves the 'get' half of the interface
 * for subtypes to implement. It is package-visible only,
 * with its methods being public for exposure by its subtypes.
 * <p>
 */
abstract class NoPutResultSetImpl
extends BasicNoPutResultSetImpl
{
	/* Set in constructor and not modified */
	public final int				resultSetNumber;

	// fields used when being called as a RowSource
	private boolean needsRowLocation;
	protected ExecRow clonedExecRow;
	protected TargetResultSet	targetResultSet;

	/* beetle 4464. compact flags into array of key column positions that we do check/skip nulls,
	 * so that we burn less cycles for each row, column.
	 */
	protected int[] checkNullCols;
	protected int cncLen;

	/**
	 *  Constructor
	 *
	 *	@param	activation			The activation
	 *	@param	resultSetNumber		The resultSetNumber
	 *  @param	optimizerEstimatedRowCount	The optimizer's estimated number
	 *										of rows.
	 *  @param	optimizerEstimatedCost		The optimizer's estimated cost
	 */
	NoPutResultSetImpl(Activation activation,
						int resultSetNumber,
						double optimizerEstimatedRowCount,
						double optimizerEstimatedCost)
	{
		super(null,
				activation,
				optimizerEstimatedRowCount,
				optimizerEstimatedCost);

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(activation!=null, "activation expected to be non-null");
			SanityManager.ASSERT(resultSetNumber >= 0, "resultSetNumber expected to be >= 0");
		}
		this.resultSetNumber = resultSetNumber;
	}

	// NoPutResultSet interface

	/**
     * Returns the description of the table's rows
	 */
	public ResultDescription getResultDescription() {
	    return activation.getResultDescription();
	}

	/**
		Return my cursor name for JDBC. Can be null.
	*/
	public String getCursorName() {

		String cursorName = activation.getCursorName();
		if ((cursorName == null) && isForUpdate()) {

			activation.setCursorName(activation.getLanguageConnectionContext().getUniqueCursorName());

			cursorName = activation.getCursorName();
		}

		return cursorName;
	}

	/** @see NoPutResultSet#resultSetNumber() */
	public int resultSetNumber() {
		return resultSetNumber;
	}

	/**
		Close needs to invalidate any dependent statements, if this is a cursor.
		Must be called by any subclasses that override close().
		@exception StandardException on error
	*/
	public void close() throws StandardException
	{
		if (!isOpen)
			return;

		/* If this is the top ResultSet then we must
		 * close all of the open subqueries for the
		 * entire query.
		 */
		if (isTopResultSet)
		{
			/*
			** If run time statistics tracing is turned on, then now is the
			** time to dump out the information.
			*/
			LanguageConnectionContext lcc = getLanguageConnectionContext();

			int staLength = (subqueryTrackingArray == null) ? 0 :
								subqueryTrackingArray.length;

			for (int index = 0; index < staLength; index++)
			{
				if (subqueryTrackingArray[index] == null)
				{
					continue;
				}
				if (subqueryTrackingArray[index].isClosed())
				{
					continue;
				}
				subqueryTrackingArray[index].close();
			}
		}

		isOpen = false;

	}

	/** @see NoPutResultSet#setTargetResultSet */
	public void setTargetResultSet(TargetResultSet trs)
	{
		targetResultSet = trs;
	}

	/** @see NoPutResultSet#setNeedsRowLocation */
	public void setNeedsRowLocation(boolean needsRowLocation)
	{
		this.needsRowLocation = needsRowLocation;
	}

	// RowSource interface
	
	/** 
	 * @see RowSource#getValidColumns
	 */
	public FormatableBitSet getValidColumns()
	{
		// All columns are valid
		return null;
	}
	
	/** 
	 * @see RowSource#getNextRowFromRowSource
	 * @exception StandardException on error
	 */
	public DataValueDescriptor[] getNextRowFromRowSource()
		throws StandardException
	{
		ExecRow execRow = getNextRowCore();
 		if (execRow != null)
		{
			/* Let the target preprocess the row.  For now, this
			 * means doing an in place clone on any indexed columns
			 * to optimize cloning and so that we don't try to drain
			 * a stream multiple times.  This is where we also
			 * enforce any check constraints.
			 */
			clonedExecRow = targetResultSet.preprocessSourceRow(execRow);

			return execRow.getRowArray();
		}

		return null;
	}

	/**
	 * @see RowSource#needsToClone
	 */
	public boolean needsToClone()
	{
		return(true);
	}

	/** 
	 * @see RowSource#closeRowSource
	 */
	public void closeRowSource()
	{
		// Do nothing here - actual work will be done in close()
	}


	// RowLocationRetRowSource interface

	/**
	 * @see RowLocationRetRowSource#needsRowLocation
	 */
	public boolean needsRowLocation()
	{
		return needsRowLocation;
	}

	/**
	 * @see RowLocationRetRowSource#rowLocation
	 * @exception StandardException on error
	 */
	public void rowLocation(RowLocation rl)
		throws StandardException
	{
		targetResultSet.changedRow(clonedExecRow, rl);
	}


	// class implementation

	/**
	 * Clear the Orderable cache for each qualifier.
	 * (This should be done each time a scan/conglomerate with
	 * qualifiers is reopened.)
	 *
	 * @param qualifiers	The Qualifiers to clear
	 */
	protected void clearOrderableCache(Qualifier[][] qualifiers) throws StandardException
	{
		// Clear the Qualifiers's Orderable cache 
		if (qualifiers != null)
		{
			Qualifier qual;
            for (Qualifier[] qualifier : qualifiers) {
                for (int index = 0; index < qualifier.length; index++) {
                    qual = qualifier[index];
                    qual.clearOrderableCache();
                    /* beetle 4880 performance enhancement and avoid deadlock while pushing
					 * down method call to store: pre-evaluate.
					 */
                    if (((GenericQualifier) qual).variantType != Qualifier.VARIANT)
                        qual.getOrderable();        // ignore return value
                }
            }
		}
	}

	/**
	 * Set the current row to the row passed in.
	 *
	 * @param row the new current row
	 *
	 */
	public final void setCurrentRow(ExecRow row)
	{
		activation.setCurrentRow(row, resultSetNumber);
		currentRow = row;
	}

	/**
	 * Clear the current row
	 *
	 */
	public void clearCurrentRow()
	{
		currentRow = null;
		activation.clearCurrentRow(resultSetNumber);
	}

	/**
	 * Is this ResultSet or it's source result set for update
	 * This method will be overriden in the inherited Classes
	 * if it is true
	 * @return Whether or not the result set is for update.
	 */
	public boolean isForUpdate()
	{
		return false;
	}

	/**
	 * Return true if we should skip the scan due to nulls in the start
	 * or stop position when the predicate on the column(s) in question
	 * do not implement ordered null semantics. beetle 4464, we also compact
	 * the areNullsOrdered flags into checkNullCols here.
	 *
	 * @param startPosition	An index row for the start position
	 * @param stopPosition	An index row for the stop position
	 *
	 * @return	true means not to do the scan
	 */
	protected boolean skipScan(ExecIndexRow startPosition, ExecIndexRow stopPosition)
		throws StandardException
	{
		int nStartCols = (startPosition == null) ? 0 : startPosition.nColumns();
		int nStopCols = (stopPosition == null) ? 0 : stopPosition.nColumns();

		/* Two facts 1) for start and stop key column positions, one has to be the prefix
		 * of the other, 2) startPosition.areNullsOrdered(i) can't be different from
		 * stopPosition.areNullsOrdered(i) unless the case "c > null and c < 5", (where c is
		 * non-nullable), in which we skip the scan anyway.
		 * So we can just use the longer one to get checkNullCols.
		 */
		boolean startKeyLonger = false;
		int size = nStopCols;
		if (nStartCols > nStopCols)
		{
			startKeyLonger = true;
			size = nStartCols;
		}
		if (size == 0)
			return false;
		if ((checkNullCols == null) || (checkNullCols.length < size))
			checkNullCols = new int[size];
		cncLen = 0;

		boolean returnValue = false;
		for (int position = 0; position < nStartCols; position++)
		{
			if ( ! startPosition.areNullsOrdered(position))
			{
				if (startKeyLonger)
					checkNullCols[cncLen++] = position + 1;
				if (startPosition.getColumn(position + 1).isNull())
				{
					returnValue =  true;
					if (! startKeyLonger)
						break;
				}
			}
		}
		if (startKeyLonger && returnValue)
			return true;
		for (int position = 0; position < nStopCols; position++)
		{
			if ( ! stopPosition.areNullsOrdered(position))
			{
				if (! startKeyLonger)
					checkNullCols[cncLen++] = position + 1;
				if (returnValue)
					continue;
				if (stopPosition.getColumn(position + 1).isNull())
				{
					returnValue =  true;
					if (startKeyLonger)
						break;
				}
			}
		}

		return returnValue;
	}

	/**
	 * Return true if we should skip the scan due to nulls in the row
	 * when the start or stop positioners on the columns containing
	 * null do not implement ordered null semantics.
	 *
	 * @param row	An index row
	 *
	 * @return	true means skip the row because it has null
	 */
	protected boolean skipRow(ExecRow row)  throws StandardException
	{
		for (int i = 0; i < cncLen; i++)
		{
			if (row.getColumn(checkNullCols[i]).isNull())
				return true;
		}

		return false;
	}

	/**
	 * Return a 2-d array of Qualifiers as a String
	 */
	public static String printQualifiers(Qualifier[][] qualifiers)
	{
		String idt = "";

		StringBuilder output = new StringBuilder();
		if (qualifiers == null)
		{
			return idt + MessageService.getTextMessage(SQLState.LANG_NONE);
		}

        for (int term = 0; term < qualifiers.length; term++)
        {
            for (int i = 0; i < qualifiers[term].length; i++)
            {
                Qualifier qual = qualifiers[term][i];

                output = new StringBuilder(idt + output +
                        MessageService.getTextMessage(
                                SQLState.LANG_COLUMN_ID_ARRAY,
                                String.valueOf(term), String.valueOf(i)) +
                        ": " + qual.getColumnId() + "\n");
                    
                int operator = qual.getOperator();
                String opString = null;
                switch (operator)
                {
                  case Orderable.ORDER_OP_EQUALS:
                    opString = "=";
                    break;

                  case Orderable.ORDER_OP_LESSOREQUALS:
                    opString = "<=";
                    break;

                  case Orderable.ORDER_OP_LESSTHAN:
                    opString = "<";
                    break;

                  default:
                    if (SanityManager.DEBUG)
                    {
                        SanityManager.THROWASSERT("Unknown operator " + operator);
                    }

                    // NOTE: This does not have to be internationalized, because
                    // this code should never be reached.
                    opString = "unknown value (" + operator + ")";
                    break;
                }
                output.append(idt).append(MessageService.getTextMessage(SQLState.LANG_OPERATOR)).append(": ").append(opString).append("\n").append(idt).append(MessageService.getTextMessage(
                        SQLState.LANG_ORDERED_NULLS)).append(": ").append(qual.getOrderedNulls()).append("\n").append(idt).append(MessageService.getTextMessage(
                        SQLState.LANG_UNKNOWN_RETURN_VALUE)).append(": ").append(qual.getUnknownRV()).append("\n").append(idt).append(MessageService.getTextMessage(
                        SQLState.LANG_NEGATE_COMPARISON_RESULT)).append(": ").append(qual.negateCompareResult()).append("\n");
            }
        }

		return output.toString();
	}

	/**
	 * @see NoPutResultSet#updateRow
	 *
	 * This method is result sets used for scroll insensitive updatable 
	 * result sets for other result set it is a no-op.
	 */
	public void updateRow(ExecRow row, RowChanger rowChanger)
			throws StandardException {
		// Only ResultSets of type Scroll Insensitive implement
		// detectability, so for other result sets this method
		// is a no-op
	}

	/**
	 * @see NoPutResultSet#markRowAsDeleted
	 *
	 * This method is result sets used for scroll insensitive updatable 
	 * result sets for other result set it is a no-op.
	 */
	public void markRowAsDeleted() throws StandardException {
		// Only ResultSets of type Scroll Insensitive implement
		// detectability, so for other result sets this method
		// is a no-op
	}

	/**
	 * @see NoPutResultSet#positionScanAtRowLocation
	 *
	 * This method is result sets used for scroll insensitive updatable 
	 * result sets for other result set it is a no-op.
	 */
	public void positionScanAtRowLocation(RowLocation rl) 
		throws StandardException 
	{
		// Only ResultSets of type Scroll Insensitive implement
		// detectability, so for other result sets this method
		// is a no-op
	}


}
