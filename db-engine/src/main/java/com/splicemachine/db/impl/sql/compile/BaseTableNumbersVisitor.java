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

import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.util.JBitSet;

import com.splicemachine.db.iapi.error.StandardException;

/**
 * Walk through a subtree and build a list of the assigned numbers for
 * all tables that exist in that subtree.  We do this by looking for any
 * column references in the subtree and, for each column reference, we
 * walk down the ColumnReference-ResultColumn chain until we find the
 * the bottom-most table number, which should correspond to a base
 * table.
 */
public class BaseTableNumbersVisitor implements Visitor
{
	// JBitSet to hold the table numbers that we find.
	private JBitSet tableMap;

	/* Column number of the ColumnReference or ResultColumn
	 * for which we most recently found a base table number. 
	 * In cases where this visitor is only expected to find
	 * a single base table number, this field is useful for
	 * determining what the column position w.r.t. the found
	 * base table was.
	 */
	private int columnNumber;
	private boolean doNotAllowLimitNAndWinFunc = false;
	private boolean stopTraversing = false;

	/**
	 * Constructor: takes a JBitSet to use as the holder for any base table
	 * numbers found while walking the subtree.
	 *
	 * @param tableMap JBitSet into which we put the table numbers we find.
	 */
	public BaseTableNumbersVisitor(JBitSet tableMap)
	{
		this.tableMap = tableMap;
		columnNumber = -1;
	}

	public BaseTableNumbersVisitor(JBitSet tableMap, boolean doNotAllowLimitNAndWinFunc)
	{
		this.tableMap = tableMap;
		columnNumber = -1;
		this.doNotAllowLimitNAndWinFunc = doNotAllowLimitNAndWinFunc;
	}
	/**
	 * Set a new JBitSet to serve as the holder for base table numbers
	 * we find while walking.
	 *
	 * @param tableMap JBitSet into which we put the table numbers we find.
	 */
	public void setTableMap(JBitSet tableMap)
	{
		this.tableMap = tableMap;
	}

	/**
	 * Reset the state of this visitor.
	 */
	protected void reset()
	{
		tableMap.clearAll();
		columnNumber = -1;
		stopTraversing = false;
	}

	/**
	 * Retrieve the the position of the ColumnReference or
	 * ResultColumn for which we most recently found a base
	 * table number.
	 */
	protected int getColumnNumber()
	{
		return columnNumber;
	}

	////////////////////////////////////////////////
	//
	// VISITOR INTERFACE
	//
	////////////////////////////////////////////////

	@Override
	public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
		ResultColumn rc = null;
		if (node instanceof ColumnReference)
		{
			// Start by seeing if this column reference is the
			// bottom-most one, meaning that there are no column
			// references beneath this one.
			rc = ((ColumnReference)node).getSource();

			if (rc == null) {
			// this can happen if column reference is pointing to a column
			// that is not from a base table.  For example, if we have a
			// VALUES clause like
			//
			//    (values (1, 2), (3, 4)) V1 (i, j)
			//
			// and then a column reference to VI.i, the column reference
			// won't have a source.
				return node;
			}
		}
		else if (node instanceof ResultColumn)
			rc = (ResultColumn)node;
		else if (node instanceof SelectNode)
		{
			if (doNotAllowLimitNAndWinFunc) {
				boolean noPush = false;
				// no limit n/top n
				if (((SelectNode) node).offset != null || ((SelectNode) node).fetchFirst != null) {
					noPush = true;
				}

				// we should not push join condition underneath a window function.
				// this restriction may be relaxed in the future if the join column is a partition by column
				if (((SelectNode)node).hasWindows()) {
					noPush = true;
				}

				if (noPush) {
					stopTraversing = true;
					tableMap.clearAll();
					columnNumber = -1;
					return node;
				}
			}

			// If the node is a SelectNode we just need to look at its
			// FROM list.
			((SelectNode)node).getFromList().accept(this);
		}
		else if (node instanceof FromBaseTable) {
		// just grab the FBT's table number.
			tableMap.set(((FromBaseTable)node).getTableNumber());
		}

		if (rc != null)
		{
			// This next call will walk through the ResultColumn tree
			// until it finds another ColumnReference, and then will
			// return the table number for that column reference.  We
			// can't stop there, though, because the column reference
			// that we found might in turn have column references beneath
			// it, and we only want the table number of the bottom-most
			// column reference.  So once we find the column reference,
			// we have to recurse.

			int baseTableNumber = rc.getTableNumber();
			if (baseTableNumber >= 0) {
			// Move down to the column reference that has the table
			// number that we just found.  There may be one or more
			// VirtualColumnNode-to-ResultColumnNode links between
			// the current ResultColumn and the column reference we're
			// looking for, so we have to walk past those until we find
			// the desired column reference.

				ValueNode rcExpr = rc.getExpression();
				while (rcExpr instanceof VirtualColumnNode) {
					rc = ((VirtualColumnNode)rcExpr).getSourceColumn();
					rcExpr = rc.getExpression();
				}

				if (rcExpr instanceof ColumnReference)
				// we found our column reference; recurse using that.
					rcExpr.accept(this);
				else {
				// Else we must have found the table number someplace
				// other than within a ColumnReference (ex. we may
				// have pulled it from a VirtualColumnNode's source
				// table); so just set the number.
					tableMap.set(baseTableNumber);
					columnNumber = rc.getColumnPosition();
				}
			}
			else if (node instanceof ColumnReference) {
			// we couldn't find any other table numbers beneath the
			// ColumnReference, so just use the table number for
			// that reference.
				ColumnReference cr = (ColumnReference)node;
				cr.getTablesReferenced(tableMap);
				columnNumber = cr.getColumnNumber();
			}
		}

		return node;
	}

	/**
	 * @see com.splicemachine.db.iapi.sql.compile.Visitor#skipChildren
	 */
	public boolean skipChildren(Visitable node)
	{
		/* A SelectNode's children can include a where clause in the
		 * form of either a PredicateList or an AndNode.  In either
		 * case we don't want to descend into the where clause because
		 * it's possible that it references a base table that is not
		 * in the subtree we're walking.  So we skip the children of
		 * a SelectNode.  Similarly, any other PredicateList may contain
		 * references to base tables that we don't want to include, so
		 * we skip a PredicateList's children as well.  Note, though,
		 * that if this visitor is specifically targeted for a particular
		 * Predicate or AndNode (i.e. a call is directly made to
		 * Predicate.accept() or AndNode.accept()) then we _will_ descend
		 * into that predicate's operands and retrieve referenced base
		 * table numbers.
		 *
		 * And finally, if we visit a FromBaseTable we can just grab
		 * it's number and that's it--there's no need to go any further.
		 */
		return (node instanceof FromBaseTable) ||
			(node instanceof SelectNode) ||
			(node instanceof PredicateList);
	}

	/**
	 * @see com.splicemachine.db.iapi.sql.compile.Visitor#stopTraversal
	 */
	public boolean stopTraversal()
	{
		return stopTraversing;
	}

	/**
	 * @see com.splicemachine.db.iapi.sql.compile.Visitor#visitChildrenFirst
	 */
	public boolean visitChildrenFirst(Visitable node)
	{
		return false;
	}

}	
