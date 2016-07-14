/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */
package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;

/**
 * Replaces a <em>source</em> expression with a <em>target</em>
 * expression.  
 * 
 */
class SubstituteExpressionVisitor implements Visitor 
{
    protected int numSubstitutions;
	protected ValueNode source;
	protected ValueNode target;
	private Class     skipOverClass;
	
	SubstituteExpressionVisitor(
			ValueNode s, ValueNode t, Class skipThisClass)  
	{
		source = s;
		target = t;
		skipOverClass = skipThisClass;
	}

	/**
	 * used by GroupByNode to process expressions by complexity level.
	 */
	public ValueNode getSource()
	{
		return source;
	}

    @Override
	public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException
	{
		if (!(node instanceof ValueNode))
		{
			return node;
		}
		
		ValueNode nd = (ValueNode)node;
		if (nd.isEquivalent(source)) 
		{
            ++numSubstitutions;
			return target;
		} 
		else 
		{
			return node;
		}
	}

    public int getNumSubstitutions() {
        return numSubstitutions;
    }

	public boolean stopTraversal() 
	{
		return false;
	}

	public boolean skipChildren(Visitable node) 
	{
		return (skipOverClass == null) ?
				false:
				skipOverClass.isInstance(node);
	}

	public boolean visitChildrenFirst(Visitable node)
	{
		return false;
	}
}
