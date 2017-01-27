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

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;

import com.splicemachine.db.iapi.types.TypeId;

import com.splicemachine.db.iapi.types.DataTypeDescriptor;

import com.splicemachine.db.iapi.sql.compile.TypeCompiler;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.reference.ClassName;

import java.util.List;

/**
 * This node represents a binary arithmetic operator, like + or *.
 *
 */

public final class BinaryArithmeticOperatorNode extends BinaryOperatorNode
{
	/**
	 * Initializer for a BinaryArithmeticOperatorNode
	 *
	 * @param leftOperand	The left operand
	 * @param rightOperand	The right operand
	 */

	public void init(
					Object leftOperand,
					Object rightOperand)
	{
		super.init(leftOperand, rightOperand,
				ClassName.NumberDataValue, ClassName.NumberDataValue);
	}

	public void setNodeType(int nodeType)
	{
		String operator = null;
		String methodName = null;

		switch (nodeType)
		{
			case C_NodeTypes.BINARY_DIVIDE_OPERATOR_NODE:
				operator = TypeCompiler.DIVIDE_OP;
				methodName = "divide";
				break;

			case C_NodeTypes.BINARY_MINUS_OPERATOR_NODE:
				operator = TypeCompiler.MINUS_OP;
				methodName = "minus";
				break;

			case C_NodeTypes.BINARY_PLUS_OPERATOR_NODE:
				operator = TypeCompiler.PLUS_OP;
				methodName = "plus";
				break;

			case C_NodeTypes.BINARY_TIMES_OPERATOR_NODE:
				operator = TypeCompiler.TIMES_OP;
				methodName = "times";
				break;

			case C_NodeTypes.MOD_OPERATOR_NODE:
				operator = TypeCompiler.MOD_OP;
				methodName = "mod";
				break;

			default:
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
	public ValueNode bindExpression( FromList	fromList, SubqueryList subqueryList, List<AggregateNode> aggregateVector) throws StandardException {
		super.bindExpression(fromList, subqueryList, aggregateVector);

		TypeId	leftType = leftOperand.getTypeId();
		TypeId	rightType = rightOperand.getTypeId();
		DataTypeDescriptor	leftDTS = leftOperand.getTypeServices();
		DataTypeDescriptor	rightDTS = rightOperand.getTypeServices();

		/* Do any implicit conversions from (long) (var)char. */
		if (leftType.isStringTypeId() && rightType.isNumericTypeId())
		{
			boolean nullableResult;
			nullableResult = leftDTS.isNullable() ||
		 					 rightDTS.isNullable();
			/* If other side is decimal/numeric, then we need to diddle
			 * with the precision, scale and max width in order to handle
			 * computations like:  1.1 + '0.111'
			 */
			int precision = rightDTS.getPrecision();
			int scale	  = rightDTS.getScale();
			int maxWidth  = rightDTS.getMaximumWidth();

			if (rightType.isDecimalTypeId())
			{
				int charMaxWidth = leftDTS.getMaximumWidth();
				precision += (2 * charMaxWidth);								
				scale += charMaxWidth;								
				maxWidth = precision + 3;
			}

			leftOperand = (ValueNode)
					getNodeFactory().getNode(
						C_NodeTypes.CAST_NODE,
						leftOperand, 
						new DataTypeDescriptor(rightType, precision,
											scale, nullableResult, 
											maxWidth),
						getContextManager());
			((CastNode) leftOperand).bindCastNodeOnly();
		}
		else if (rightType.isStringTypeId() && leftType.isNumericTypeId())
		{
			boolean nullableResult;
			nullableResult = leftDTS.isNullable() ||
		 					 rightDTS.isNullable();
			/* If other side is decimal/numeric, then we need to diddle
			 * with the precision, scale and max width in order to handle
			 * computations like:  1.1 + '0.111'
			 */
			int precision = leftDTS.getPrecision();
			int scale	  = leftDTS.getScale();
			int maxWidth  = leftDTS.getMaximumWidth();

			if (leftType.isDecimalTypeId())
			{
				int charMaxWidth = rightDTS.getMaximumWidth();
				precision += (2 * charMaxWidth);								
				scale += charMaxWidth;								
				maxWidth = precision + 3;
			}

			rightOperand =  (ValueNode)
					getNodeFactory().getNode(
						C_NodeTypes.CAST_NODE,
						rightOperand, 
						new DataTypeDescriptor(leftType, precision,
											scale, nullableResult, 
											maxWidth),
						getContextManager());
			((CastNode) rightOperand).bindCastNodeOnly();
		}

		/*
		** Set the result type of this operator based on the operands.
		** By convention, the left operand gets to decide the result type
		** of a binary operator.
		*/
		setType(leftOperand.getTypeCompiler().
					resolveArithmeticOperation(
						leftOperand.getTypeServices(),
						rightOperand.getTypeServices(),
						operator
							)
				);

		return this;
	}
}
