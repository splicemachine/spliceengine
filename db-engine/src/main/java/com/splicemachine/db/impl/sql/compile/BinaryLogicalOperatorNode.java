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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

import com.splicemachine.db.iapi.services.classfile.VMOpcode;

import java.util.List;

abstract class BinaryLogicalOperatorNode extends BinaryOperatorNode
{
	boolean	shortCircuitValue;

	/**
	 * Initializer for a BinaryLogicalOperatorNode
	 *
	 * @param leftOperand	The left operand of the comparison
	 * @param rightOperand	The right operand of the comparison
	 * @param methodName	The name of the method to call in the generated
	 *						class.  In this case, it's actually an operator
	 *						name.
	 */

	public void init(
				Object	leftOperand,
				Object	rightOperand,
				Object		methodName)
	{
		/* For logical operators, the operator and method names are the same */
		super.init(leftOperand, rightOperand, methodName, methodName,
				ClassName.BooleanDataValue, ClassName.BooleanDataValue);
	}

	/**
	 * Bind this logical operator.  All that has to be done for binding
	 * a logical operator is to bind the operands, check that both operands
	 * are BooleanDataValue, and set the result type to BooleanDataValue.
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
                                    List<AggregateNode> aggregateVector) throws StandardException {
		//following is to check if we have something like "? AND 1=1" or "2>1 OR ?" 
		if (leftOperand.isParameterNode() || rightOperand.isParameterNode())
			throw StandardException.newException(SQLState.LANG_NON_BOOLEAN_WHERE_CLAUSE, "PARAMETER" );

		super.bindExpression(fromList, subqueryList, aggregateVector);

		return this;
	}

	/**
	 * Verify that eliminateNots() did its job correctly.  Verify that
	 * there are no NotNodes above the top level comparison operators
	 * and boolean expressions.
	 *
	 * @return		Boolean which reflects validity of the tree.
	 */
	boolean verifyEliminateNots()
	{
		if (SanityManager.ASSERT)
		{
			return (leftOperand.verifyEliminateNots() &&
					rightOperand.verifyEliminateNots());
		}
		else
		{
			return true;
		}
	}

	/**
	 * Do code generation for this logical binary operator.
	 * This is used for AND and OR. the IsNode extends this class but
	 * overrides generateExpression.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the code to place the code
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
		throws StandardException
	{		
		/*
		** This generates the following code:
		**
		** (<leftOperand>.equals(shortCircuitValue) ?
		**	 <leftOperand> :
		**   <leftOperand>.<and/or>(<rightOperand>)
		**
		** The ?: operator accomplishes the short-circuiting.  We save the
		** value of the left operand on the stack so we don't have to evaluate
		** it twice.
		**
		** The BooleanDataValue.{and,or} methods return an immutable BooleanDataValue
		** and an immutable BooleanDataValue is returned by this generated code in
		** the short circuit case.
		*/

		/*
		** See whether the left operand equals the short-circuit value.
		** Generated code is:
		**		.equals(shortCircuitValue)
		*/

		leftOperand.generateExpression(acb, mb);
		// stack - left

		// put an extra left of the stack for potential
		// use in the else clause.
		mb.dup();
		// stack - left, left
		mb.push(shortCircuitValue);
		// stack - left, left, shortcircuit
		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "equals", "boolean", 1);
		// stack left, result

		/*
		** Generate the if expression.  This is what accomplishes
		** short-circuiting.
		**
		** Generated code is:
		**
		**		<test for short circuiting> ?
		**			<call to BooleanDataValue.getImmutable> : <call to operator method>
		**
		** For AND short circuiting shortcircuit value will be false, so that
		** if left is false, no need to evaluate the right and the result will be false.
		**
		** For OR short circuiting shortcircuit value will be true, so that
		** if left is true, no need to to evaluate the right and the result will be true.
		**
		** In both cases the result is the same as the left operand.
		**
		** TODO: Could short circuit when the left value is NULL as well. Then
		** the result would be NULL in either case and still equal to the left value.
		** This would require a different check on the conditional.
		*/

		mb.conditionalIf();
		
		// stack: left
		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getImmutable",
				ClassName.BooleanDataValue, 0);
		
		// stack: result (matching left)

		mb.startElseCode();

		/*
		** Generate the return value if the left operand does not equal the
		** short-circuit value.  This is the call to "and" or "or".
		**
		** Generated code is:
		**
		**	<fieldx>.<methodName>(<rightOperand>)
		*/

		// stack: left

		rightOperand.generateExpression(acb, mb);

		// stack: left, right
		mb.upCast(ClassName.BooleanDataValue);

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, methodName, ClassName.BooleanDataValue, 1);
		// stack: result(left op right)

		mb.completeConditional();
		//	 stack: result

	}

	DataTypeDescriptor resolveLogicalBinaryOperator(
								DataTypeDescriptor leftType,
								DataTypeDescriptor rightType)
							throws StandardException
	{
		if ( ( ! (leftType.getTypeId().isBooleanTypeId()) ) ||
			 ( ! (rightType.getTypeId().isBooleanTypeId()) ) )
		{
			throw StandardException.newException(SQLState.LANG_BINARY_LOGICAL_NON_BOOLEAN);
		}

		return leftType.getNullabilityType(
					leftType.isNullable() || rightType.isNullable());
	}
}
