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

package com.splicemachine.db.impl.sql.compile;

import java.util.List;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.types.TypeId;

/**
 * A GroupByColumn is a column in the GROUP BY clause.
 *
 */
public class GroupByColumn extends OrderedColumn 
{
	private ValueNode columnExpression;
	
	/**
	 * Initializer.
	 *
	 * @param colRef	The ColumnReference for the grouping column
	 */
	public void init(Object colRef) 
	{
		this.columnExpression = (ValueNode)colRef;
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */

	public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			if (columnExpression != null)
			{
				printLabel(depth, "columnExpression: ");
				columnExpression.treePrint(depth + 1);
			}
		}
	}

	/**
	 * Get the name of this column
	 *
	 * @return	The name of this column
	 */
	public String getColumnName() 
	{
		return columnExpression.getColumnName();
	}

	/**
	 * Bind this grouping column.
	 *
	 * @param fromList			The FROM list to use for binding
	 * @param subqueryList		The SubqueryList we are building as we hit
	 *							SubqueryNodes.
	 * @param aggregateVector	The aggregate vector we build as we hit 
	 *							AggregateNodes.
	 *
	 * @exception StandardException	Thrown on error
	 */

	public void bindExpression(FromList fromList,
                               SubqueryList subqueryList,
                               List<AggregateNode> aggregateVector)  throws StandardException {
		/* Bind the ColumnReference to the FromList */
        int previousReliability = orReliability( CompilerContext.GROUP_BY_RESTRICTION );
		columnExpression =columnExpression.bindExpression(fromList,
							  subqueryList,
							  aggregateVector);
        getCompilerContext().setReliability( previousReliability );

		// Verify that we can group on the column
		if (columnExpression.isParameterNode()) 
		{
			throw StandardException.newException(SQLState.LANG_INVALID_COL_REF_GROUPED_SELECT_LIST,
					columnExpression);
		}
		/*
		 * Do not check to see if we can map user types
		 * to built-in types.  The ability to do so does
		 * not mean that ordering will work.  In fact,
		 * as of version 2.0, ordering does not work on
		 * user types.
		 */
		TypeId ctid = columnExpression.getTypeId();
		if (! ctid.orderable(getClassFactory()))
		{
			throw StandardException.newException(SQLState.LANG_COLUMN_NOT_ORDERABLE_DURING_EXECUTION, 
							ctid.getSQLTypeName());
		}
	}

	@Override
    public ValueNode getColumnExpression()
	{
		return columnExpression;
	}

	@Override
    public void setColumnExpression(ValueNode cexpr)
	{
		this.columnExpression = cexpr;
		
	}

	/**
	 * Accept the visitor for all visitable children of this node.
	 *
     * @param v the visitor
     */
    @Override
	public void acceptChildren(Visitor v) throws StandardException {
		super.acceptChildren(v);
		if (columnExpression != null) {
			columnExpression = (ValueNode)columnExpression.accept(v, this);
		}
	}
}
