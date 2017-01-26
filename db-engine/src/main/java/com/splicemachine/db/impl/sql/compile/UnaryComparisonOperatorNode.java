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
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.util.JBitSet;

import java.util.List;

/**
 * This node is the superclass  for all unary comparison operators, such as is null
 * and is not null.
 *
 */

public abstract class UnaryComparisonOperatorNode extends UnaryOperatorNode implements RelationalOperator {
	/**
	 * Bind this comparison operator.  All that has to be done for binding
	 * a comparison operator is to bind the operand and set the result type 
	 * to SQLBoolean.
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
		bindOperand(fromList, subqueryList,  aggregateVector);

		/* Set type info for this node */
		bindComparisonOperator();

		return this;
	}

	/**
	 * Set the type info for this node.  This method is useful both during 
	 * binding and when we generate nodes within the language module outside 
	 * of the parser.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindComparisonOperator() throws StandardException {
		/*
		** Set the result type of this comparison operator based on the
		** operand.  The result type is always SQLBoolean and always
		** non-nullable.
		*/
		setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID, false));
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
	@Override
	ValueNode eliminateNots(boolean underNotNode)  throws StandardException {
		if (! underNotNode) {
			return this;
		}

		/* Convert the BinaryComparison operator to its negation */
		return getNegation(operand);
	}

	/**
	 * Negate the comparison.
	 *
	 * @param operand	The operand of the comparison operator
	 *
	 * @return BinaryOperatorNode	The negated expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	abstract UnaryOperatorNode getNegation(ValueNode operand) throws StandardException;

	/* RelationalOperator interface */

	@Override
	public ColumnReference getColumnOperand( Optimizable optTable, int columnPosition) {
		FromBaseTable	ft;

		assert optTable instanceof FromBaseTable;

		ft = (FromBaseTable) optTable;
		ColumnReference	cr;
		if (operand instanceof ColumnReference) {
			/*
			** The operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) operand;
			if (cr.getTableNumber() == ft.getTableNumber()) {
				/* The table is correct, how about the column position? */
				if (cr.getSource().getColumnPosition() == columnPosition) {
					/* We've found the correct column - return it */
					return cr;
				}
			}
		}

		/* Neither side is the column we're looking for */
		return null;
	}

	@Override
	public ColumnReference getColumnOperand(Optimizable optTable) {
		ColumnReference	cr;

		if (operand instanceof ColumnReference) {
			/*
			** The operand is a column reference.
			** Is it the correct column?
			*/
			cr = (ColumnReference) operand;
			if (cr.getTableNumber() == optTable.getTableNumber()) {
				/* We've found the correct column - return it */
				return cr;
			}
		}

		/* Not the column we're looking for */
		return null;
	}

	@Override
	public ValueNode getOperand(ColumnReference cRef, int refSetSize, boolean otherSide) {
		if (otherSide)
		// there is no "other" side for Unary, so just return null.
			return null;

		ColumnReference	cr;
		if (operand instanceof ColumnReference) {
			/*
			** The operand is a column reference.
			** Is it the correct column?
			*/
			JBitSet cRefTables = new JBitSet(refSetSize);
			JBitSet crTables = new JBitSet(refSetSize);
			BaseTableNumbersVisitor btnVis =
				new BaseTableNumbersVisitor(crTables);

			cr = (ColumnReference) operand;
			try {
				cr.accept(btnVis);
				btnVis.setTableMap(cRefTables);
				cRef.accept(btnVis);
			} catch (StandardException se) {
            	if (SanityManager.DEBUG) {
            	    SanityManager.THROWASSERT("Failed when trying to " +
            	        "find base table number for column reference check:",
						se);
            	}
			}
			crTables.and(cRefTables);
			if (crTables.getFirstSetBit() != -1) {
				/*
				** The table is correct, how about the column position?
				*/
				if (cr.getSource().getColumnPosition() == cRef.getColumnNumber()) {
					/* We've found the correct column - return it. */
					return operand;
				}
			}
		}

		/* Not the column we're looking for */
		return null;
	}

	@Override
	public boolean selfComparison(ColumnReference cr) {
		assert cr==operand: "ColumnReference not found in IsNullNode.";

		/* An IsNullNode is not a comparison with any other column */
		return false;
	}

	@Override
	public ValueNode getExpressionOperand(int tableNumber, int columnNumber, FromTable ft) {
		return null;
	}

	@Override
	public void generateExpressionOperand(Optimizable optTable,
										  int columnPosition,
										  ExpressionClassBuilder acb,
										  MethodBuilder mb) throws StandardException {
		acb.generateNull(mb, operand.getTypeCompiler(),  operand.getTypeServices().getCollationType(),
				operand.getTypeServices().getPrecision(),operand.getTypeServices().getScale());
	}

	/** @see RelationalOperator#getStartOperator */
	@Override
	public int getStartOperator(Optimizable optTable) {
		if (SanityManager.DEBUG) {
			SanityManager.THROWASSERT( "getStartOperator not expected to be called for " + this.getClass().getName());
		}

		return ScanController.GE;
	}

	@Override
	public int getStopOperator(Optimizable optTable) {
		if (SanityManager.DEBUG) {
			SanityManager.THROWASSERT( "getStopOperator not expected to be called for " + this.getClass().getName());
		}

		return ScanController.GT;
	}

	/** @see RelationalOperator#generateOrderedNulls */
	public void generateOrderedNulls(MethodBuilder mb)
	{
		mb.push(true);
	}

	@Override
	public void generateQualMethod(ExpressionClassBuilder acb,
								   MethodBuilder mb,
								   Optimizable optTable) throws StandardException {
		MethodBuilder qualMethod = acb.newUserExprFun();

		/* Generate a method that returns that expression */
		acb.generateNull(qualMethod, operand.getTypeCompiler(),
				operand.getTypeServices().getCollationType(),
				operand.getTypeServices().getPrecision(),
				operand.getTypeServices().getScale());
		qualMethod.methodReturn();
		qualMethod.complete();

		/* Return an expression that evaluates to the GeneratedMethod */
		acb.pushMethodReference(mb, qualMethod);
	}

	@Override
	public void generateAbsoluteColumnId(MethodBuilder mb, Optimizable optTable) {
		// Get the absolute 0-based column position for the column
		int columnPosition = getAbsoluteColumnPosition(optTable);
		mb.push(columnPosition);

        int storagePosition=getAbsoluteStoragePosition(optTable);
        mb.push(storagePosition);
	}

	@Override
	public void generateRelativeColumnId(MethodBuilder mb, Optimizable optTable) {
		// Get the absolute 0-based column position for the column
		int columnPosition = getAbsoluteColumnPosition(optTable);
		// Convert the absolute to the relative 0-based column position
		columnPosition = optTable.convertAbsoluteToRelativeColumnPosition(columnPosition);
		mb.push(columnPosition);

        int storagePosition=getAbsoluteStoragePosition(optTable);
        storagePosition=optTable.convertAbsoluteToRelativeColumnPosition(storagePosition);
        mb.push(storagePosition);
    }

	/**
	 * Get the absolute 0-based column position of the ColumnReference from 
	 * the conglomerate for this Optimizable.
	 *
	 * @param optTable	The Optimizable
	 *
	 * @return The absolute 0-based column position of the ColumnReference
	 */
	private int getAbsoluteColumnPosition(Optimizable optTable) {
		ColumnReference	cr = (ColumnReference) operand;
		int columnPosition;
		ConglomerateDescriptor bestCD;

		/* Column positions are one-based, store is zero-based */
		columnPosition = cr.getSource().getColumnPosition();

		bestCD = optTable.getTrulyTheBestAccessPath().getConglomerateDescriptor();

		/*
		** If it's an index, find the base column position in the index
		** and translate it to an index column position.
		*/
		if (bestCD != null && bestCD.isIndex()) {
			columnPosition = bestCD.getIndexDescriptor().getKeyColumnPosition(columnPosition);
            assert columnPosition>0: "Base column not found in index";
		}

		// return the 0-based column position
		return columnPosition - 1;
	}

    private int getAbsoluteStoragePosition(Optimizable optTable) {
        ColumnReference	cr = (ColumnReference) operand;
        int columnPosition;
        ConglomerateDescriptor bestCD;

        bestCD = optTable.getTrulyTheBestAccessPath().getConglomerateDescriptor();

		/*
		** If it's an index, find the base column position in the index
		** and translate it to an index column position.
		*/
        if (bestCD != null && bestCD.isIndex()) {
            columnPosition = cr.getSource().getColumnPosition();
            columnPosition = bestCD.getIndexDescriptor().getKeyColumnPosition(columnPosition);
            assert columnPosition>0: "Base column not found in index";
        } else {
            columnPosition = cr.getSource().getStoragePosition();
        }

        // return the 0-based column position
        return columnPosition - 1;
    }

    @Override
	public boolean orderedNulls() { return true; }

	@Override
	public boolean isQualifier(Optimizable optTable, boolean forPush) {
		/*
		** It's a Qualifier if the operand is a ColumnReference referring
		** to a column in the given Optimizable table.
		*/
		if ( ! (operand instanceof ColumnReference))
			return false;

		ColumnReference cr = (ColumnReference) operand;
		FromTable ft = (FromTable) optTable;

		return cr.getTableNumber()==ft.getTableNumber();
	}

	@Override
	public int getOrderableVariantType(Optimizable optTable)  throws StandardException {
		return operand.getOrderableVariantType();
	}
}
