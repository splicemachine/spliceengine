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

import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.error.StandardException;

/**
 * If a RCL (SELECT list) contains an aggregate, then we must verify
 * that the RCL (SELECT list) is valid.  
 * For ungrouped queries,
 * the RCL must be composed entirely of valid aggregate expressions -
 * in this case, no column references outside of an aggregate.
 * For grouped aggregates,
 * the RCL must be composed of grouping columns or valid aggregate
 * expressions - in this case, the only column references allowed outside of
 * an aggregate are grouping columns.
 *
 */
public class VerifyAggregateExpressionsVisitor implements Visitor
{
	private GroupByList groupByList;

	public VerifyAggregateExpressionsVisitor(GroupByList groupByList)
	{
		this.groupByList = groupByList;
	}


	////////////////////////////////////////////////
	//
	// VISITOR INTERFACE
	//
	////////////////////////////////////////////////

	/**
	 * Verify that this expression is ok
	 * for an aggregate query.  
	 *
	 * @param node 	the node to process
	 *
	 * @return me
	 *
	 * @exception StandardException on ColumnReference not
	 * 	in group by list, ValueNode or	
	 * 	JavaValueNode that isn't under an
	 * 	aggregate
	 */
    @Override
	public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
		if (node instanceof ColumnReference)
		{
			ColumnReference cr = (ColumnReference)node;
		
			if (groupByList == null)
			{
				throw StandardException.newException(SQLState.LANG_INVALID_COL_REF_NON_GROUPED_SELECT_LIST, cr.getSQLColumnName());
			}

			if (groupByList.findGroupingColumn(cr) == null)
			{
				throw StandardException.newException(SQLState.LANG_INVALID_COL_REF_GROUPED_SELECT_LIST, cr.getSQLColumnName());
			}
		} 
		
		/*
		** Subqueries are only valid if they do not have
		** correlations and are expression subqueries.  RESOLVE:
		** this permits VARIANT expressions in the subquery --
		** should this be allowed?  may be confusing to
		** users to complain about:
		**
		**	select max(x), (select sum(y).toString() from y) from x
		*/
		else if (node instanceof SubqueryNode)
		{
			SubqueryNode subq = (SubqueryNode)node;
		
			if ((subq.getSubqueryType() != SubqueryNode.EXPRESSION_SUBQUERY) ||
				 subq.hasCorrelatedCRs())
			{
				throw StandardException.newException( (groupByList == null) ?
							SQLState.LANG_INVALID_NON_GROUPED_SELECT_LIST :
							SQLState.LANG_INVALID_GROUPED_SELECT_LIST);
			}

			/*
			** TEMPORARY RESTRICTION: we cannot handle an aggregate
			** in the subquery 
			*/
			HasNodeVisitor visitor = new HasNodeVisitor(AggregateNode.class);
			subq.accept(visitor);
			if (visitor.hasNode())
			{	
				throw StandardException.newException( (groupByList == null) ?
							SQLState.LANG_INVALID_NON_GROUPED_SELECT_LIST :
							SQLState.LANG_INVALID_GROUPED_SELECT_LIST);
			}
		}
		return node;
	}

	/**
	 * Don't visit children under an aggregate, subquery or any node which
	 * is equivalent to any of the group by expressions.
	 *
	 * @param node 	the node to process
	 *
	 * @return true/false
	 * @throws StandardException 
	 */
	public boolean skipChildren(Visitable node) throws StandardException 
	{
		return ((node instanceof AggregateNode) ||
				(node instanceof SubqueryNode) ||
				(node instanceof ValueNode &&
						groupByList != null 
						&& groupByList.findGroupingColumn((ValueNode)node) != null));
	}
	
	public boolean stopTraversal()
	{
		return false;
	}

	public boolean visitChildrenFirst(Visitable node)
	{
		return false;
	}
}	
