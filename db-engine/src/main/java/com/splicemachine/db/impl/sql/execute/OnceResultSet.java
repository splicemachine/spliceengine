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

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.sql.conn.StatementContext;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;

import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.sql.Activation;

import com.splicemachine.db.iapi.services.loader.GeneratedMethod;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.error.StandardException;

/**
 * Takes an expression subquery's result set and verifies that only
 * a single scalar value is being returned.
 * NOTE: A row with a single column containing null will be returned from
 * getNextRow() if the underlying subquery ResultSet is empty.
 *
 */
public class OnceResultSet extends NoPutResultSetImpl
{
	/* Statics for cardinality check */
	public static final int DO_CARDINALITY_CHECK		= 1;
	public static final int NO_CARDINALITY_CHECK		= 2;
	public static final int UNIQUE_CARDINALITY_CHECK	= 3;

	/* Used to cache row with nulls for case when subquery result set
	 * is empty.
	 */
	private ExecRow rowWithNulls;

	/* Used to cache the StatementContext */
	private StatementContext statementContext;

    // set in constructor and not altered during
    // life of object.
    public NoPutResultSet source;
	private GeneratedMethod emptyRowFun;
	private int cardinalityCheck;
	public int subqueryNumber;
	public int pointOfAttachment;

    //
    // class interface
    //
    public OnceResultSet(NoPutResultSet s, Activation a, GeneratedMethod emptyRowFun,
						 int cardinalityCheck, int resultSetNumber,
						 int subqueryNumber, int pointOfAttachment,
						 double optimizerEstimatedRowCount,
						 double optimizerEstimatedCost)
	{
		super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        source = s;
		this.emptyRowFun = emptyRowFun;
		this.cardinalityCheck = cardinalityCheck;
		this.subqueryNumber = subqueryNumber;
		this.pointOfAttachment = pointOfAttachment;
		recordConstructorTime();
    }

	//
	// ResultSet interface (leftover from NoPutResultSet)
	//

	/**
     * open a scan on the table. scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...
	 *
	 * @exception StandardException thrown if cursor finished.
     */
	public void	openCore() throws StandardException 
	{
		/* NOTE: We can't get code generation
		 * to generate calls to reopenCore() for
		 * subsequent probes, so we just handle
		 * it here.
		 */
		if (isOpen)
		{
			reopenCore();
			return;
		}

		beginTime = getCurrentTimeMillis();

        source.openCore();

		/* Notify StatementContext about ourself so that we can
		 * get closed down, if necessary, on an exception.
		 */
		if (statementContext == null)
		{
			statementContext = getLanguageConnectionContext().getStatementContext();
		}
		statementContext.setSubqueryResultSet(subqueryNumber, this, 
											  activation.getNumSubqueries());

		numOpens++;
	    isOpen = true;
		openTime += getElapsedMillis(beginTime);
	}

	/**
	 * reopen a scan on the table. scan parameters are evaluated
	 * at each open, so there is probably some way of altering
	 * their values...
	 *
	 * @exception StandardException thrown if cursor finished.
	 */
	public void	reopenCore() throws StandardException 
	{
		beginTime = getCurrentTimeMillis();
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT(isOpen, "OnceResultSet already open");

        source.reopenCore();
		numOpens++;

		openTime += getElapsedMillis(beginTime);
	}

	/**
     * Return the requested value computed from the next row.  
	 *
	 * @exception StandardException thrown on failure.
	 *			  StandardException ScalarSubqueryCardinalityViolation
	 *						Thrown if scalar subquery returns more than 1 row.
	 */
	public ExecRow	getNextRowCore() throws StandardException 
	{
	    ExecRow candidateRow = null;
		ExecRow secondRow = null;
	    ExecRow result = null;

		beginTime = getCurrentTimeMillis();
		// This is an ASSERT and not a real error because this is never
		// outermost in the tree and so a next call when closed will not occur.
		if (SanityManager.DEBUG)
        	SanityManager.ASSERT( isOpen, "OpenResultSet not open");

	    if ( isOpen ) 
		{
			candidateRow = source.getNextRowCore();

			if (candidateRow != null)
			{
				switch (cardinalityCheck)
				{
					case DO_CARDINALITY_CHECK:
					case NO_CARDINALITY_CHECK:
						candidateRow = candidateRow.getClone();
						if (cardinalityCheck == DO_CARDINALITY_CHECK)
						{
							/* Raise an error if the subquery returns > 1 row 
							 * We need to make a copy of the current candidateRow since
							 * the getNextRow() for this check will wipe out the underlying
							 * row.
							 */
							secondRow = source.getNextRowCore();
							if (secondRow != null)
							{
								close();
								throw StandardException.newException(SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION);
							}
						}
						result = candidateRow;
						break;

					case UNIQUE_CARDINALITY_CHECK:
						candidateRow = candidateRow.getClone();
						secondRow = source.getNextRowCore();
						DataValueDescriptor orderable1 = candidateRow.getColumn(1);
						while (secondRow != null)
						{
							DataValueDescriptor orderable2 = secondRow.getColumn(1);
							if (! (orderable1.compare(DataValueDescriptor.ORDER_OP_EQUALS, orderable2, true, true)))
							{
								close();
								throw StandardException.newException(SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION);
							}
							secondRow = source.getNextRowCore();
						}
						result = candidateRow;
						break;

					default:
						if (SanityManager.DEBUG)
						{
							SanityManager.THROWASSERT(
								"cardinalityCheck not unexpected to be " +
								cardinalityCheck);
						}
						break;
				}
			}
			else if (rowWithNulls == null)
			{
				rowWithNulls = (ExecRow) emptyRowFun.invoke(activation);
				result = rowWithNulls;
			}
			else
			{
				result = rowWithNulls;
			}
	    }

		setCurrentRow(result);
		rowsSeen++;

		nextTime += getElapsedMillis(beginTime);
	    return result;
	}

	/**
	 * If the result set has been opened,
	 * close the open scan.
	 *
	 * @exception StandardException thrown on error
	 */
	public void	close() throws StandardException
	{
		beginTime = getCurrentTimeMillis();
	    if ( isOpen ) 
		{
			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
		    clearCurrentRow();

	        source.close();

			super.close();
	    }
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of OnceResultSet repeated");

		closeTime += getElapsedMillis(beginTime);
	}

	/**
	 * @see NoPutResultSet#getPointOfAttachment
	 */
	public int getPointOfAttachment()
	{
		return pointOfAttachment;
	}

	/**
	 * Return the total amount of time spent in this ResultSet
	 *
	 * @param type	CURRENT_RESULTSET_ONLY - time spent only in this ResultSet
	 *				ENTIRE_RESULTSET_TREE  - time spent in this ResultSet and below.
	 *
	 * @return long		The total amount of time spent (in milliseconds).
	 */
	public long getTimeSpent(int type)
	{
		long totTime = constructorTime + openTime + nextTime + closeTime;

		if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY)
		{
			return	totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
		}
		else
		{
			return totTime;
		}
	}
}
