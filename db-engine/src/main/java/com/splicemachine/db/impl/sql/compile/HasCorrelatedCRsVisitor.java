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

/**
 * Find out if we have an correlated column reference
 * anywhere below us.  Stop traversal as soon as we find one.
 *
 */
public class HasCorrelatedCRsVisitor implements Visitor
{
	private boolean hasCorrelatedCRs;

	/**
	 * Construct a visitor
	 */
	public HasCorrelatedCRsVisitor()
	{
	}



	////////////////////////////////////////////////
	//
	// VISITOR INTERFACE
	//
	////////////////////////////////////////////////

	/**
	 * If we have found the target node, we are done.
	 *
	 * @param node 	the node to process
	 *
	 * @return me
	 */
    @Override
	public Visitable visit(Visitable node, QueryTreeNode parent)
	{
		if (node instanceof ColumnReference)
		{
			if (((ColumnReference)node).getCorrelated())
			{
				hasCorrelatedCRs = true;
			}
		}
		else if (node instanceof VirtualColumnNode)
		{
			if (((VirtualColumnNode)node).getCorrelated())
			{
				hasCorrelatedCRs = true;
			}
		}
		else if (node instanceof MethodCallNode)
		{
			/* trigger action references are correlated
			 */
			if (((MethodCallNode)node).getMethodName().equals("getTriggerExecutionContext") ||
				((MethodCallNode)node).getMethodName().equals("TriggerOldTransitionRows") ||
				((MethodCallNode)node).getMethodName().equals("TriggerNewTransitionRows")
			   )
			{
				hasCorrelatedCRs = true;
			}
		}
		return node;
	}

	/**
	 * Stop traversal if we found the target node
	 *
	 * @return true/false
	 */
	public boolean stopTraversal()
	{
		return hasCorrelatedCRs;
	}

	public boolean skipChildren(Visitable v)
	{
		return false;
	}

	public boolean visitChildrenFirst(Visitable v)
	{
		return false;
	}

	////////////////////////////////////////////////
	//
	// CLASS INTERFACE
	//
	////////////////////////////////////////////////
	/**
	 * Indicate whether we found the node in
	 * question
	 *
	 * @return true/false
	 */
	public boolean hasCorrelatedCRs()
	{
		return hasCorrelatedCRs;
	}

	/**
	 * Shortcut to set if hasCorrelatedCRs
	 *
	 *	@param	value	true/false
	 */
	public void setHasCorrelatedCRs(boolean value)
	{
		hasCorrelatedCRs = value;
	}
}
