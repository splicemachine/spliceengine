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
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.services.context.ContextManager;

/**
 * A BetweenOperatorNode represents a BETWEEN clause. The between values are
 * represented as a 2 element list in order to take advantage of code reuse.
 *
 */

public class BetweenOperatorNode extends BinaryListOperatorNode
{
	/**
	 * Initializer for a BetweenOperatorNode
	 *
	 * @param leftOperand		The left operand of the node
	 * @param betweenValues		The between values in list form
	 */

	public void init(Object leftOperand, Object betweenValues)
	{
		if (SanityManager.DEBUG)
		{
			ValueNodeList betweenVals = (ValueNodeList) betweenValues;

			SanityManager.ASSERT(betweenVals.size() == 2,
				"betweenValues.size() (" +
				betweenVals.size()	+
				") is expected to be 2");
		}

		super.init(leftOperand, betweenValues, "BETWEEN", null);
	}

	/**
	 * Eliminate NotNodes in the current query block.  We traverse the tree, 
	 * inverting ANDs and ORs and eliminating NOTs as we go.  We stop at 
	 * ComparisonOperators and boolean expressions.  We invert 
	 * ComparisonOperators and replace boolean expressions with 
	 * boolean expression = false.
	 * NOTE: Since we do not recurse under ComparisonOperators, there
	 * still could be NotNodes left in the tree.
	 *
	 * @param	underNotNode		Whether or not we are under a NotNode.
	 *							
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	ValueNode eliminateNots(boolean underNotNode) 
					throws StandardException
	{
		BinaryComparisonOperatorNode leftBCO;
		BinaryComparisonOperatorNode rightBCO;
		OrNode						 newOr;

		if (SanityManager.DEBUG)
		SanityManager.ASSERT(rightOperandList.size() == 2,
			"rightOperandList.size() (" +
			rightOperandList.size()	+
			") is expected to be 2");

		if (! underNotNode)
		{
			return this;
		}

		/* we want to convert the BETWEEN  * into < OR > 
		   as described below.
		*/		

		/* Convert:
		 *		leftO between rightOList.elementAt(0) and rightOList.elementAt(1)
		 * to:
		 *		leftO < rightOList.elementAt(0) or leftO > rightOList.elementAt(1)
		 * NOTE - We do the conversion here since ORs will eventually be
		 * optimizable and there's no benefit for the optimizer to see NOT BETWEEN
		 */

		NodeFactory nodeFactory = getNodeFactory();
		ContextManager cm = getContextManager();

		/* leftO < rightOList.elementAt(0) */
		leftBCO = (BinaryComparisonOperatorNode) 
					nodeFactory.getNode(
									C_NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE,
									leftOperand, 
								 	rightOperandList.elementAt(0),
									cm);
		/* Set type info for the operator node */
		leftBCO.bindComparisonOperator();

        // DERBY-4388: If leftOperand is a ColumnReference, it may be remapped
        // during optimization, and that requires the less-than node and the
        // greater-than node to have separate objects.
        ValueNode leftClone = (leftOperand instanceof ColumnReference) ?
            leftOperand.getClone() : leftOperand;

		/* leftO > rightOList.elementAt(1) */
		rightBCO = (BinaryComparisonOperatorNode) 
					nodeFactory.getNode(
								C_NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE,
								leftClone,
								rightOperandList.elementAt(1),
								cm);
		/* Set type info for the operator node */
		rightBCO.bindComparisonOperator();

		/* Create and return the OR */
		newOr = (OrNode) nodeFactory.getNode(
												C_NodeTypes.OR_NODE,
												leftBCO,
												rightBCO,
												cm);
		newOr.postBindFixup();

		/* Tell optimizer to use the between selectivity instead of >= * <= selectivities */
		leftBCO.setBetweenSelectivity();
		rightBCO.setBetweenSelectivity();

		return newOr;
	}

	/**
	 * Preprocess an expression tree.  We do a number of transformations
	 * here (including subqueries, IN lists, LIKE and BETWEEN) plus
	 * subquery flattening.
	 * NOTE: This is done before the outer ResultSetNode is preprocessed.
	 *
	 * @param	numTables			Number of tables in the DML Statement
	 * @param	outerFromList		FromList from outer query block
	 * @param	outerSubqueryList	SubqueryList from outer query block
	 * @param	outerPredicateList	PredicateList from outer query block
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ValueNode preprocess(int numTables,
								FromList outerFromList,
								SubqueryList outerSubqueryList,
								PredicateList outerPredicateList) 
					throws StandardException
	{
		ValueNode	leftClone1;
		ValueNode	rightOperand;

		/* We must 1st preprocess the component parts */
		super.preprocess(numTables,
						 outerFromList, outerSubqueryList,
						 outerPredicateList);

		/* This is where we do the transformation for BETWEEN to make it optimizable.
		 * c1 BETWEEN value1 AND value2 -> c1 >= value1 AND c1 <= value2
		 * This transformation is only done if the leftOperand is a ColumnReference.
		 */
		if (!(leftOperand instanceof ColumnReference))
		{
			return this;
		}

		/* For some unknown reason we need to clone the leftOperand if it is
		 * a ColumnReference because reusing them in Qualifiers for a scan
		 * does not work.  
		 */
		leftClone1 = leftOperand.getClone();

		/* The transformed tree has to be normalized:
		 *				AND
		 *			   /   \
		 *			  >=    AND
		 *				   /   \
		 *				  <=    TRUE
		 */

		NodeFactory nodeFactory = getNodeFactory();
		ContextManager cm = getContextManager();

        QueryTreeNode trueNode = (QueryTreeNode) nodeFactory.getNode(
											C_NodeTypes.BOOLEAN_CONSTANT_NODE,
											Boolean.TRUE,
											cm);

		/* Create the AND <= */
		BinaryComparisonOperatorNode lessEqual = 
			(BinaryComparisonOperatorNode) nodeFactory.getNode(
						C_NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE,
						leftClone1, 
						rightOperandList.elementAt(1),
						cm);

		/* Set type info for the operator node */
		lessEqual.bindComparisonOperator();

		/* Create the AND */
		AndNode newAnd = (AndNode) nodeFactory.getNode(
												C_NodeTypes.AND_NODE,
												lessEqual,
												trueNode,
												cm);
		newAnd.postBindFixup();

		/* Create the AND >= */
		BinaryComparisonOperatorNode greaterEqual = 
			(BinaryComparisonOperatorNode) nodeFactory.getNode(
					C_NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE,
					leftOperand, 
					rightOperandList.elementAt(0),
					cm);

		/* Set type info for the operator node */
		greaterEqual.bindComparisonOperator();

		/* Create the AND */
		newAnd = (AndNode) nodeFactory.getNode(
												C_NodeTypes.AND_NODE,
												greaterEqual,
												newAnd,
												cm);
		newAnd.postBindFixup();

		/* Tell optimizer to use the between selectivity instead of >= * <= selectivities */
		lessEqual.setBetweenSelectivity();
		greaterEqual.setBetweenSelectivity();

		return newAnd;
	}
 
	/**
	 * Do code generation for this BETWEEN operator.
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
		AndNode						 newAnd;
		BinaryComparisonOperatorNode leftBCO;
		BinaryComparisonOperatorNode rightBCO;

		if (SanityManager.DEBUG)
		SanityManager.ASSERT(rightOperandList.size() == 2,
			"rightOperandList.size() (" +
			rightOperandList.size()	+
			") is expected to be 2");

		/* Convert:
		 *		leftO between rightOList.elementAt(0) and rightOList.elementAt(1)
		 * to:
		 *		leftO >= rightOList.elementAt(0) and leftO <= rightOList.elementAt(1) 
		 */

		NodeFactory nodeFactory = getNodeFactory();
		ContextManager cm = getContextManager();

		/* leftO >= rightOList.elementAt(0) */
		leftBCO = (BinaryComparisonOperatorNode) 
					nodeFactory.getNode(
							C_NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE,
							leftOperand, 
							rightOperandList.elementAt(0),
							cm);
		/* Set type info for the operator node */
		leftBCO.bindComparisonOperator();

		/* leftO <= rightOList.elementAt(1) */
		rightBCO = (BinaryComparisonOperatorNode) 
					nodeFactory.getNode(
						C_NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE,
						leftOperand, 
						rightOperandList.elementAt(1),
						cm);
		/* Set type info for the operator node */
		rightBCO.bindComparisonOperator();

		/* Create and return the AND */
		newAnd = (AndNode) nodeFactory.getNode(
												C_NodeTypes.AND_NODE,
												leftBCO,
												rightBCO,
												cm);
		newAnd.postBindFixup();
		newAnd.generateExpression(acb, mb);
	}
	
	public int hashCode(){
		int result = leftOperand==null? 0: leftOperand.hashCode();
		result = 31*result+(rightOperandList==null?0:rightOperandList.hashCode());
		return result;
	}
}
