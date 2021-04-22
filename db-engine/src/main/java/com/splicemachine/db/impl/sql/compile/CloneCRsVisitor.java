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
 * All such Splice Machine modifications are Copyright 2012 - 2021 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.error.StandardException;

import static com.splicemachine.db.shared.common.reference.SQLState.LANG_INTERNAL_ERROR;

/**
 * Shallow clone any ColumnReference nodes in the tree if its
 * parent node is cloneable, and clone the parent node as well,
 * to make sure there is no node sharing of ColumnReferences.
 * If the parent of any ColumnReference is not cloneable,
 * throw a StandardException.
 * Also, clone any other cloneable nodes to prevent duplicates.
 * One usage of this is to avoid identical copies of Predicates during DNF to CNF
 * so that pushing and remapping one copy of the Predicate doesn't affect
 * the other copy.
 * Note: This is a special-purpose visitor for cloning boolean SQL expressions.
 *       It does not currently handle SubqueryNodes.  If one is seen, a
 *       StandardException is thrown.
 *
 */

public class CloneCRsVisitor implements Visitor
{
	private boolean copySourceOfCR = false;
	private boolean initializeSourceOfCR = false;

	public CloneCRsVisitor()
	{
	}
	////////////////////////////////////////////////
	//
	// VISITOR INTERFACE
	//
	////////////////////////////////////////////////

	public Visitable visit(Visitable node, QueryTreeNode parent)
		throws StandardException
	{
		boolean isCR = false;
		if (node instanceof ColumnReference)
		{
			isCR = true;
			if (!parent.isCloneable())
				throw StandardException.newException(LANG_INTERNAL_ERROR,
                    "CloneCRsVisitor encountered a ColumnReference which couldn't be cloned.");
		}
		else if (node instanceof ResultColumnList)
			throw StandardException.newException(LANG_INTERNAL_ERROR,
				"CloneCRsVisitor encountered unexpected ResultColumnList");
		else if (node instanceof SubqueryNode)
			throw StandardException.newException(LANG_INTERNAL_ERROR,
				"CloneCRsVisitor encountered unexpected SubqueryNode");
		QueryTreeNode queryTreeNode = (QueryTreeNode)node;
		if (queryTreeNode.isCloneable()) {
			QueryTreeNode clone;
			if (queryTreeNode instanceof OrNode) {
				if (parent instanceof OrNode)
					return node;
				OrNode orNode = (OrNode)queryTreeNode;
				clone = orNode.shallowCloneORChain();
			}
			else if (queryTreeNode instanceof AndNode) {
				if (parent instanceof AndNode)
					return node;
				AndNode andNode = (AndNode)queryTreeNode;
				clone = andNode.shallowCloneANDChain();
			}
			else
			    clone = queryTreeNode.getClone();
			if (isCR) {
				ColumnReference cRef = (ColumnReference)clone;
				// Scoped columns can do multiple levels of remapping, so let's
				// use this option to be safe.
				cRef.markAsScoped();
				if (initializeSourceOfCR)
					cRef.setSource(null);
				else if (copySourceOfCR)
					cRef.setSource(cRef.getSource().cloneMe());
			}
			return clone;
		}

	    return node;
	}

	public void setCopySourceOfCR(boolean copySourceOfCR) {
		this.copySourceOfCR = copySourceOfCR;
	}

	public void setInitializeSourceOfCR(boolean initializeSourceOfCR) {
		this.initializeSourceOfCR = initializeSourceOfCR;
	}

	public boolean skipChildren(Visitable node) { return false; }

	public boolean visitChildrenFirst(Visitable node)
	{
		return false;
	}

	public boolean stopTraversal()
	{
		return false;
	}

	public boolean visitChildrenOfNewParent() { return true; }
	////////////////////////////////////////////////
	//
	// CLASS INTERFACE
	//
	////////////////////////////////////////////////
}	
