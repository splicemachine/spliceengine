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

import com.splicemachine.db.iapi.error.StandardException;

/**
 * Remap/unremap the CRs to the underlying
 * expression.
 *
 */
public class RemapCRsVisitor implements Visitor
{
	private boolean remap;

	public RemapCRsVisitor(boolean remap)
	{
		this.remap = remap;
	}


	////////////////////////////////////////////////
	//
	// VISITOR INTERFACE
	//
	////////////////////////////////////////////////

	/**
	 * Don't do anything unless we have a ColumnReference
	 * node.
	 *
	 * @param node 	the node to process
	 *
	 * @return me
	 *
	 * @exception StandardException on error
	 */
	public Visitable visit(Visitable node, QueryTreeNode parent)
		throws StandardException
	{
		/*
		 * Remap all of the ColumnReferences in this expression tree
		 * to point to the ResultColumn that is 1 level under their
		 * current source ResultColumn.
		 * This is useful for pushing down single table predicates.
		 */
		if (node instanceof ColumnReference)
		{
			ColumnReference cr = (ColumnReference) node;
			if (remap)
			{
				cr.remapColumnReferences();
			}
			else
			{
				cr.unRemapColumnReferences();
			}
		}

		return node;
	}

	/**
	 * No need to go below a SubqueryNode.
	 *
	 * @return Whether or not to go below the node.
	 */
	public boolean skipChildren(Visitable node)
	{
		return (node instanceof SubqueryNode);
	}

	public boolean visitChildrenFirst(Visitable node)
	{
		return false;
	}

	public boolean stopTraversal()
	{
		return false;
	}

	////////////////////////////////////////////////
	//
	// CLASS INTERFACE
	//
	////////////////////////////////////////////////

}	
