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

import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

import java.util.List;
import java.util.Vector;
/**
 * An UntypedNullConstantNode represents a SQL NULL before it has
 * been bound.  The bind() operation will replace the UntypedNullConstantNodes
 * with typed ConstantNodes.
 */

public final class UntypedNullConstantNode extends ConstantNode
{
	/**
	 * Constructor for an UntypedNullConstantNode.  Untyped constants
	 * contain no state (not too surprising).
	 */

	public UntypedNullConstantNode()
	{
		super();
	}

	/**
	 * Return the length
	 *
	 * @return	The length of the value this node represents
	 *
	 */

	//public int	getLength()
	//{
	//	if (SanityManager.DEBUG)
	//	SanityManager.ASSERT(false,
	//	  "Unimplemented method - should not be called on UntypedNullConstantNode");
	//	return 0;
	//}

	/**
	 * Should never be called for UntypedNullConstantNode because
	 * we shouldn't make it to generate
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the expression will go into
	 */
	void generateConstant(ExpressionClassBuilder acb, MethodBuilder mb)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("generateConstant() not expected to be called for UntypedNullConstantNode because we have implemented our own generateExpression().");
		}
	}

	/**
	 * Translate a Default node into a default value, given a type descriptor.
	 *
	 * @param typeDescriptor	A description of the required data type.
	 *
	 */
	public DataValueDescriptor convertDefaultNode(DataTypeDescriptor typeDescriptor)
	throws StandardException
	{
		/*
		** The default value is null, so set nullability to TRUE
		*/
		return typeDescriptor.getNull();
	}
	
	/** @see ValueNode#bindExpression(FromList, SubqueryList, Vector)
	 * @see ResultColumnList#bindUntypedNullsToResultColumns
	 * This does nothing-- the node is actually bound when
	 * bindUntypedNullsToResultColumns is called.
	 */
	public ValueNode bindExpression(FromList fromList, SubqueryList	subqueryList, List<AggregateNode> aggregateVector)
	{
		return this;
	}
	
	public int hashCode(){
		return value==null? 0: value.hashCode();
	}
}
