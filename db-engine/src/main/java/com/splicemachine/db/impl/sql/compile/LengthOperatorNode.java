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

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;

import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.impl.sql.catalog.Aggregate;

import java.sql.Types;

import java.util.List;
import java.util.Vector;

/**
 * This node represents a unary XXX_length operator
 *
 */

public final class LengthOperatorNode extends UnaryOperatorNode
{
	private int parameterType;
	private int parameterWidth;

	public void setNodeType(int nodeType)
	{
		String operator = null;
		String methodName = null;

		if (nodeType == C_NodeTypes.CHAR_LENGTH_OPERATOR_NODE)
		{
				operator = "char_length";
				methodName = "charLength";
				parameterType = Types.VARCHAR;
				parameterWidth = TypeId.VARCHAR_MAXWIDTH;
		}
		else
		{
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"Unexpected nodeType = " + nodeType);
				}
		}
		setOperator(operator);
		setMethodName(methodName);
		super.setNodeType(nodeType);
	}

	/**
	 * Bind this operator
	 *
	 * @param fromList			The query's FROM list
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
	 * @param aggregateVector	The aggregate vector being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
	@Override
	public ValueNode bindExpression(FromList fromList,
									SubqueryList subqueryList,
									List<AggregateNode> aggregateVector) throws StandardException{
		TypeId	operandType;

		bindOperand(fromList, subqueryList,
				aggregateVector);

		/*
		** Check the type of the operand - this function is allowed only on
		** string value types.  
		*/
		operandType = operand.getTypeId();
		switch (operandType.getJDBCTypeId())
		{
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.BINARY:
				case Types.VARBINARY:
				case Types.LONGVARBINARY:
				case Types.LONGVARCHAR:
                case Types.BLOB:
                case Types.CLOB:
					break;
			
				default:
					throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE,
											getOperatorString(),
											operandType.getSQLTypeName());
		}

		/*
		** The result type of XXX_length is int.
		*/
		setType(new DataTypeDescriptor(
							TypeId.INTEGER_ID,
							operand.getTypeServices().isNullable()
						)
				);
		return this;
	}

	/**
	 * Bind a ? parameter operand of the XXX_length function.
	 *
	 * @exception StandardException		Thrown on error
	 */

	void bindParameter()
			throws StandardException
	{
		/*
		** According to the SQL standard, if XXX_length has a ? operand,
		** its type is varchar with the implementation-defined maximum length
		** for a varchar.
		** Also, for XXX_length, it doesn't matter what is VARCHAR's collation 
		** (since for XXX_length, no collation sensitive processing is 
		** is required) and hence we will not worry about the collation setting
		*/

		operand.setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor(parameterType, true, 
												parameterWidth));
	}

	/**
	 * This is a length operator node.  Overrides this method
	 * in UnaryOperatorNode for code generation purposes.
	 */
	public String getReceiverInterfaceName() {
	    return ClassName.ConcatableDataValue;
	}
}
