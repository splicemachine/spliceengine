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

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;


import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.error.StandardException;

/**
 * Find out if we have a value node with variant type less than what the
 * caller desires, anywhere below us.  Stop traversal as soon as we find one.
 * This is used in two places: one to check the values clause of an insert
 * statement; i.e 
 * <pre>
 * insert into <table> values (?, 1, foobar());
 * </pre>
 * If all the expressions in the values clause are QUERY_INVARIANT (and an
 * exception is made for parameters) then we can cache the results in the
 * RowResultNode. This is useful when we have a prepared insert statement which
 * is repeatedly executed.
 * <p>
 * The second place where this is used is to check if a subquery can be
 * materialized or not. 
 * @see com.splicemachine.db.iapi.store.access.Qualifier
 *
 */
public class HasVariantValueNodeVisitor implements Visitor
{
	private boolean hasVariant;
	private int variantType;
	private boolean ignoreParameters;


	/**
	 * Construct a visitor
	 */
	public HasVariantValueNodeVisitor()
	{
		this.variantType = Qualifier.VARIANT;
		this.ignoreParameters = false;
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(Qualifier.VARIANT < Qualifier.SCAN_INVARIANT, "qualifier constants not ordered as expected");
			SanityManager.ASSERT(Qualifier.SCAN_INVARIANT < Qualifier.QUERY_INVARIANT, "qualifier constants not ordered as expected");
		}		
	}

	
	/**
	 * Construct a visitor.  Pass in the variant
	 * type.  We look for nodes that are less
	 * than or equal to this variant type.  E.g.,
	 * if the variantType is Qualifier.SCAN_VARIANT,
	 * then any node that is either VARIANT or
	 * SCAN_VARIANT will cause the visitor to 
	 * consider it variant.
	 *
	 * @param variantType the type of variance we consider
	 *		variant
	 * @param ignoreParameters should I ignore parameter nodes?
 	 */
	public HasVariantValueNodeVisitor(int variantType, boolean ignoreParameters)
	{
		this.variantType = variantType;
		this.ignoreParameters = ignoreParameters;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(variantType >= Qualifier.VARIANT, "bad variantType");
			// note: there is no point in (variantType == Qualifier.CONSTANT) so throw an
			// exception for that case too
			SanityManager.ASSERT(variantType <= Qualifier.QUERY_INVARIANT, "bad variantType");
		}		
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
	 *
	 * @exception StandardException on error
	 */
	public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException
	{
		if (node instanceof ValueNode)
		{
			if (ignoreParameters && ((ValueNode)node).requiresTypeFromContext())
				return node;
				
			if (((ValueNode)node).getOrderableVariantType() <= variantType)
			{
				hasVariant = true;
			}
		}
		return node;
	}

	public boolean skipChildren(Visitable node)
	{
		return false;
	}

	public boolean visitChildrenFirst(Visitable node)
	{
		return false;
	}

	/**
	 * Stop traversal if we found the target node
	 *
	 * @return true/false
	 */
	public boolean stopTraversal()
	{
		return hasVariant;
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
	public boolean hasVariant()
	{
		return hasVariant;
	}
}
