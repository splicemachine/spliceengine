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

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.compiler.MethodBuilder;

import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;

import java.util.List;

public class IsNode extends BinaryLogicalOperatorNode {
	private boolean		notMe;	// set to true if we're to negate the sense of this node

	/**
	 * Initializer for an IsNode
	 *
	 * @param leftOperand	The left operand of the IS
	 * @param rightOperand	The right operand of the IS
	 * @param notMe			Whether to reverse the sense of this node.
	 */

	public void init(
							Object leftOperand,
							Object rightOperand,
							Object notMe )
	{
		// the false for shortCir
		super.init(leftOperand, rightOperand, "is" );
		this.notMe = (Boolean) notMe;
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
		super.bindExpression(fromList, subqueryList, aggregateVector);

		leftOperand.checkIsBoolean();
		rightOperand.checkIsBoolean();

		setType(leftOperand.getTypeServices());

		return this;
	}

	
	/**
	 * Eliminate NotNodes in the current query block. We just mark whether
	 * this IS node is under an eliminated NOT node.
	 *
	 * @param	underNotNode	Whether or not we are under a NotNode.
	 *							
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	ValueNode eliminateNots(boolean underNotNode) 
					throws StandardException
	{
		if ( underNotNode ) { notMe = !notMe; }

		leftOperand = leftOperand.eliminateNots( false);
		rightOperand = rightOperand.eliminateNots( false );

		return this;
	}

	/**
	 * Do the 1st step in putting child expressions into conjunctive normal
	 * form.  This step ensures that the top level of the child expression is
	 * a chain of AndNodes terminated by a true BooleanConstantNode.
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ValueNode putAndsOnTop() 
					throws StandardException
	{
		leftOperand = leftOperand.putAndsOnTop();
		rightOperand = rightOperand.putAndsOnTop();

		return this;
	}

	/**
	 * Verify that putAndsOnTop() did its job correctly.  Verify that the top level 
	 * of the expression is a chain of AndNodes terminated by a true BooleanConstantNode.
	 *
	 * @return		Boolean which reflects validity of the tree.
	 */
	public boolean verifyPutAndsOnTop()
	{
		return ( leftOperand.verifyPutAndsOnTop() && rightOperand.verifyPutAndsOnTop() );
	}

	/**
	 * Finish putting an expression into conjunctive normal
	 * form.  An expression tree in conjunctive normal form meets
	 * the following criteria:
	 *		o  If the expression tree is not null,
	 *		   the top level will be a chain of AndNodes terminating
	 *		   in a true BooleanConstantNode.
	 *		o  The left child of an AndNode will never be an AndNode.
	 *		o  Any right-linked chain that includes an AndNode will
	 *		   be entirely composed of AndNodes terminated by a true BooleanConstantNode.
	 *		o  The left child of an OrNode will never be an OrNode.
	 *		o  Any right-linked chain that includes an OrNode will
	 *		   be entirely composed of OrNodes terminated by a false BooleanConstantNode.
	 *		o  ValueNodes other than AndNodes and OrNodes are considered
	 *		   leaf nodes for purposes of expression normalization.
	 *		   In other words, we won't do any normalization under
	 *		   those nodes.
	 *
	 * In addition, we track whether or not we are under a top level AndNode.  
	 * SubqueryNodes need to know this for subquery flattening.
	 *
	 * @param	underTopAndNode		Whether or not we are under a top level AndNode.
	 *							
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ValueNode changeToCNF(boolean underTopAndNode) 
					throws StandardException
	{
		leftOperand = leftOperand.changeToCNF(false );
		rightOperand = rightOperand.changeToCNF(false );

		return this;
	}

	/**
	 * Verify that changeToCNF() did its job correctly.  Verify that:
	 *		o  AndNode  - rightOperand is not instanceof OrNode
	 *				      leftOperand is not instanceof AndNode
	 *		o  OrNode	- rightOperand is not instanceof AndNode
	 *					  leftOperand is not instanceof OrNode
	 *
	 * @return		Boolean which reflects validity of the tree.
	 */
	public boolean verifyChangeToCNF()
	{
		return	( leftOperand.verifyChangeToCNF() && rightOperand.verifyChangeToCNF() );
	}


	/**
	 * Do code generation for this logical binary operator.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb the method  the expression will go into
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
		throws StandardException
	{
		String				evaluatorMethodName;

		/*
		** Generate the return value. Generated code is:
		**
		**	<fieldLeft>.<evaluatorMethodName>(<fieldRight>)
		*/

		if ( notMe ) { evaluatorMethodName = "isNot"; }
		else { evaluatorMethodName = "is"; }

		leftOperand.generateExpression(acb, mb);
		rightOperand.generateExpression(acb, mb);
		mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.BooleanDataValue, evaluatorMethodName,
							ClassName.BooleanDataValue, 1);
	}
}
