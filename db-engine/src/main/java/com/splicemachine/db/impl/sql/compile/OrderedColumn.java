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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

/**
 * An ordered column has position.   It is an
 * abstract class for group by and order by
 * columns.
 *
 */
public abstract class OrderedColumn extends QueryTreeNode 
{
	protected static final int UNMATCHEDPOSITION = -1;
	protected int	columnPosition = UNMATCHEDPOSITION;

	/**
	 * Indicate whether this column is ascending or not.
	 * By default assume that all ordered columns are
	 * necessarily ascending.  If this class is inherited
	 * by someone that can be desceneded, they are expected
	 * to override this method.
	 *
	 * @return true
	 */
	public boolean isAscending()
	{
		return true;
	}

	/**
	 * Indicate whether this column should be ordered NULLS low.
	 * By default we assume that all ordered columns are ordered
	 * with NULLS higher than non-null values. If this class is inherited
	 * by someone that can be specified to have NULLs ordered lower than
         * non-null values, they are expected to override this method.
	 *
	 * @return false
	 */
	public boolean isNullsOrderedLow()
	{
		return false;
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */
	public String toString() 
	{
		if (SanityManager.DEBUG)
		{
			return "columnPosition: " + columnPosition + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Get the position of this column
	 *
	 * @return	The position of this column
	 */
	public int getColumnPosition() 
	{
		return columnPosition;
	}

	/**
	 * Set the position of this column
	 */
	public void setColumnPosition(int columnPosition) 
	{
		this.columnPosition = columnPosition;
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(columnPosition > 0,
				"Column position is " + columnPosition +
				". This is a problem since the code to generate " +
				" ordering columns assumes it to be one based -- i.e. "+
				" it subtracts one");

		}
	}

    public abstract ValueNode getColumnExpression();

    public abstract void setColumnExpression(ValueNode expression);

    /**
     *
     * Retrieve the column Reference from the ordered column
     *
     * @return
     */
    public ValueNode getValueNode() {
        ValueNode colExprValue = getColumnExpression();
        if (colExprValue instanceof UnaryOperatorNode)
            colExprValue = ((UnaryOperatorNode) colExprValue).getOperand();
        assert colExprValue != null && colExprValue instanceof ValueNode : "Programmer error: unexpected type or null:";
        return colExprValue;
    }

    /**
     * Compute Non-Zero Cardinality, take into account unary operators where appropriate
     *
     * @param numberOfRows
     * @return
     * @throws StandardException
     */
    public long nonZeroCardinality(long numberOfRows) throws StandardException {
        long columnReturnedRows = getValueNode().nonZeroCardinality(numberOfRows);
        if (getColumnExpression() instanceof UnaryOperatorNode) { // Adjust for extact methods, soon to push down
            UnaryOperatorNode node = (UnaryOperatorNode) getColumnExpression();
            columnReturnedRows = (node.nonZeroCardinality(numberOfRows) < columnReturnedRows)?node.nonZeroCardinality(numberOfRows):columnReturnedRows;
        }
        return columnReturnedRows;
    }
}
