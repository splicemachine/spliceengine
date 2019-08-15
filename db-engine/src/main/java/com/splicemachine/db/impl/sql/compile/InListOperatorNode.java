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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.ListDataType;
import com.splicemachine.db.iapi.types.TypeId;

import java.lang.reflect.Modifier;
import java.util.ArrayList;

/**
 * An InListOperatorNode represents an IN list.
 *
 */

public final class InListOperatorNode extends BinaryListOperatorNode
{
	private boolean isOrdered;
	private boolean sortDescending;

	/**
	 * Initializer for a InListOperatorNode
	 *
	 * @param leftOperand		The left operand of the node
	 * @param rightOperandList	The right operand list of the node
	 */

	public void init(Object leftOperand, Object rightOperandList) throws StandardException
	{
		init(leftOperand, rightOperandList, "IN", "in");
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "isOrdered: " + isOrdered + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Create a shallow copy of this InListOperatorNode whose operands are
	 * the same as this node's operands.  Copy over all other necessary
	 * state, as well.
	 */
	protected InListOperatorNode shallowCopy() throws StandardException
	{
		InListOperatorNode ilon =
			 (InListOperatorNode)getNodeFactory().getNode(
				C_NodeTypes.IN_LIST_OPERATOR_NODE,
				leftOperandList,
				rightOperandList,
				getContextManager());

		ilon.copyFields(this);
		ilon.markAsOrdered(isOrdered);

		if (sortDescending)
			ilon.markSortDescending();

		return ilon;
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
					throws StandardException {
        super.preprocess(numTables,
            outerFromList, outerSubqueryList,
            outerPredicateList);
        
        return convertToAndedEqualityProbePredicate();
    }
    
    public ValueNode convertToAndedEqualityProbePredicate()
					throws StandardException
    {
		/* Check for the degenerate case of a single element in the IN list.
		 * If found, then convert to "=".
		 */
		if (rightOperandList.size() == 1 && singleLeftOperand)
		{
			ValueNode value = (ValueNode)rightOperandList.elementAt(0);
            if ((value instanceof CharConstantNode) && (getLeftOperand() instanceof ColumnReference)) {
				DataTypeDescriptor targetType = getLeftOperand().getTypeServices();
				if (targetType.getTypeName().equals(TypeId.CHAR_NAME)) {
					int maxSize = getLeftOperand().getTypeServices().getMaximumWidth();
					ValueNodeList.rightPadCharConstantNode((CharConstantNode) value, maxSize);
				}
			}
			BinaryComparisonOperatorNode equal =
				(BinaryComparisonOperatorNode) getNodeFactory().getNode(
						C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE,
						getLeftOperand(),
						value,
						getContextManager());
			/* Set type info for the operator node */
			equal.bindComparisonOperator();
			return equal;
		}
		else if (allLeftOperandsColumnReferences() &&
			     rightOperandList.containsOnlyConstantAndParamNodes())
		{
			/* At this point we have an IN-list made up of constant and/or
			 * parameter values.  Ex.:
			 *
			 *  select id, name from emp where id in (34, 28, ?)
			 *
			 * Since the optimizer does not recognize InListOperatorNodes
			 * as potential start/stop keys for indexes, it (the optimizer)
			 * may estimate that the cost of using any of the indexes would
			 * be too high.  So we could--and probably would--end up doing
			 * a table scan on the underlying base table. But if the number
			 * of rows in the base table is significantly greater than the
			 * number of values in the IN-list, scanning the base table can
			 * be overkill and can lead to poor performance.  And further,
			 * choosing to use an index but then scanning the entire index
			 * can be slow, too. DERBY-47.
			 *
			 * What we do, then, is create an "IN-list probe predicate",
			 * which is an internally generated equality predicate with a
			 * parameter value on the right.  So for the query shown above
			 * the probe predicate would be "id = ?".  We then replace
			 * this InListOperatorNode with the probe predicate during
			 * optimization.  The optimizer in turn recognizes the probe
			 * predicate, which is disguised to look like a typical binary
			 * equality, as a potential start/stop key for any indexes.
			 * This start/stop key potential then factors into the estimated
			 * cost of probing the indexes, which leads to a more reasonable
			 * estimate and thus makes it more likely that the optimizer
			 * will choose to use an index vs a table scan.  That done, we
			 * then use the probe predicate to perform multiple execution-
			 * time "probes" on the index--instead of doing a range index
			 * scan--which eliminates unnecessary scanning. For more see
			 * execute/MultiProbeTableScanResultSet.java.
			 *
			 * With this approach we know that regardless of how large the
			 * base table is, we'll only have to probe the index a max of
			 * N times, where "N" is the size of the IN-list. If N is
			 * significantly less than the number of rows in the table, or
			 * is significantly less than the number of rows between the
			 * min value and the max value in the IN-list, this selective
			 * probing can save us a lot of time.
			 *
			 * Note: We will do fewer than N probes if there are duplicates
			 * in the list.
			 *
			 * Note also that, depending on the relative size of the IN-list
			 * verses the number of rows in the table, it may actually be
			 * better to just do a table scan--especially if there are fewer
			 * rows in the table than there are in the IN-list.  So even though
			 * we create a "probe predicate" and pass it to the optimizer, it
			 * (the optimizer) may still choose to do a table scan.  If that
			 * happens then we'll "revert" the probe predicate back to its
			 * original form (i.e. to this InListOperatorNode) during code
			 * generation, and then we'll use it as a regular IN-list
			 * restriction when it comes time to execute.
			 */

			boolean allConstants = rightOperandList.containsAllConstantNodes();

			/* If we have all constants then sort them now.  This allows us to
			 * skip the sort at execution time (we have to sort them so that
			 * we can eliminate duplicate IN-list values).  If we have one
			 * or more parameter nodes then we do *not* sort the values here
			 * because we do not (and cannot) know what values the parameter(s)
			 * will have.  In that case we'll sort the values at execution
			 * time. 
			 */
			// Disable sorting of multicolumn IN lists here for now.
			// These will typically long lists, and using bubble sort
			// is not good.  The probe values will end up getting
			// sorted, and duplicates removed, at execution time.
			if (allConstants && leftOperandList.size() == 1) {
                /* When sorting or choosing min/max in the list, if types
                 * are not an exact match then we have to use the *dominant*
                 * type across all values, where "all values" includes the
                 * left operand.  Otherwise we can end up with incorrect
                 * results.
                 *
                 * Note that it is *not* enough to just use the left operand's
                 * type as the judge because we have no guarantee that the
                 * left operand has the dominant type.  If, for example, the
                 * left operand has type INTEGER and all (or any) values in
                 * the IN list have type DECIMAL, use of the left op's type
                 * would lead to comparisons with truncated values and could
                 * therefore lead to an incorrect sort order. DERBY-2256.
                 */
                ArrayList targetTypes = new ArrayList<DataTypeDescriptor>();
                
                for (int j = 0; j < leftOperandList.size(); j++) {
                    ValueNode leftOperand = (ValueNode) leftOperandList.elementAt(j);
                    DataTypeDescriptor targetType = leftOperand.getTypeServices();
                    
                    TypeId judgeTypeId = targetType.getTypeId();
                    
                    if (!rightOperandList.allSamePrecendence(
                        judgeTypeId.typePrecedence(), j)) {
                        /* Iterate through the entire list of values to find out
                         * what the dominant type is.
                         */
                        ClassFactory cf = getClassFactory();
                        int sz = rightOperandList.size();
                        for (int i = 0; i < sz; i++) {
                            ValueNode vn = (ValueNode) rightOperandList.elementAt(i);
                            if (!singleLeftOperand)
                                vn = ((ListValueNode) vn).getValue(j);
                            targetType =
                                targetType.getDominantType(
                                    vn.getTypeServices(), cf);
                        }
                    }
                    targetTypes.add(targetType);
                }
                rightOperandList.eliminateDuplicates(targetTypes);
                
                /* Now sort the list in ascending order using the dominant
                 * type found above.
                 */
                
                DataValueDescriptor judgeODV = null;
                
                if (singleLeftOperand) {
                    judgeODV = ((DataTypeDescriptor) targetTypes.get(0)).getNull();
                }
                else {
                    ListDataType ldt = new ListDataType(leftOperandList.size());
                    for (int j = 0; j < leftOperandList.size(); j++) {
                        ldt.setFrom(((DataTypeDescriptor) targetTypes.get(j)).getNull(), j);
                    }
                    judgeODV = ldt;
                }

				rightOperandList.sortInAscendingOrder(judgeODV);
				isOrdered = true;

				if (rightOperandList.size() == 1 && singleLeftOperand)
				{
					ValueNode value = (ValueNode)rightOperandList.elementAt(0);
					BinaryComparisonOperatorNode equal =
						(BinaryComparisonOperatorNode)getNodeFactory().getNode(
							C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE,
							getLeftOperand(),
							value,
							getContextManager());
					/* Set type info for the operator node */
					equal.bindComparisonOperator();
					return equal;
				}
			}
            
            BinaryComparisonOperatorNode equal = null;
            ValueNode andNode = null;
            ValueNode lastAnd = null;
            for (int i = 0; i < leftOperandList.size(); i++) {

                /* Create a parameter node to serve as the right operand of
                 * the probe predicate.  We intentionally use a parameter node
                 * instead of a constant node because the IN-list has more than
                 * one value (some of which may be unknown at compile time, i.e.
                 * if they are parameters), so we don't want an estimate based
                 * on any single literal.  Instead we want a generic estimate
                 * of the cost to retrieve the rows matching some _unspecified_
                 * value (namely, one of the values in the IN-list, but we
                 * don't know which one).  That's exactly what a parameter
                 * node gives us.
                 *
                 * Note: If the IN-list only had a single value then we would
                 * have taken the "if (rightOperandList.size() == 1)" branch
                 * above and thus would not be here.
                 *
                 * We create the parameter node based on the first value in
                 * the list.  This is arbitrary and should not matter in the
                 * big picture.
                 */
                ValueNode srcVal = (ValueNode) rightOperandList.elementAt(0);
                ValueNode leftOperand = (ValueNode) leftOperandList.elementAt(i);
                if (srcVal instanceof ListValueNode)
                    srcVal = ((ListValueNode)srcVal).getValue(i);
                ParameterNode pNode =
                    (ParameterNode) getNodeFactory().getNode(
                        C_NodeTypes.PARAMETER_NODE,
                        0,
                        null, // default value
                        getContextManager());
    
                DataTypeDescriptor pType = srcVal.getTypeServices();
                pNode.setType(pType);
    
                /* If we choose to use the new predicate for execution-time
                 * probing then the right operand will function as a start-key
                 * "place-holder" into which we'll store the different IN-list
                 * values as we iterate through them.  This means we have to
                 * generate a valid value for the parameter node--i.e. for the
                 * right side of the probe predicate--in order to have a valid
                 * execution-time placeholder.  To do that we pass the source
                 * value from which we found the type down to the new, "fake"
                 * parameter node.  Then, when it comes time to generate the
                 * parameter node, we'll just generate the source value as our
                 * place-holder.  See ParameterNode.generateExpression().
                 *
                 * Note: the actual value of the "place-holder" does not matter
                 * because it will be clobbered by the various IN-list values
                 * (which includes "srcVal" itself) as we iterate through them
                 * during execution.
                 */
                pNode.setValueToGenerate(srcVal);
    
                /* Finally, create the "column = ?" equality that serves as the
                 * basis for the probe predicate.  We store a reference to "this"
                 * node inside the probe predicate so that, if we later decide
                 * *not* to use the probe predicate for execution time index
                 * probing, we can revert it back to its original form (i.e.
                 * to "this").
                 */

                equal =
                    (BinaryComparisonOperatorNode) getNodeFactory().getNode(
                        C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE,
                        leftOperand,
                        pNode,
                        this,
                        getContextManager());
    
                /* Set type info for the operator node */
                equal.bindComparisonOperator();
    
                // Build a chain of AND'ed binary comparisons for each
                // of the columns in a multicolumn IN list.
                if (!isSingleLeftOperand()) {
                    NodeFactory nodeFactory = getNodeFactory();
                    QueryTreeNode trueNode = (QueryTreeNode) nodeFactory.getNode(
                        C_NodeTypes.BOOLEAN_CONSTANT_NODE,
                        Boolean.TRUE,
                        getContextManager());
                    ValueNode temp;
                    temp = (AndNode) nodeFactory.getNode(
                        C_NodeTypes.AND_NODE,
                        equal,
                        trueNode,
                        getContextManager());
                    ((AndNode) temp).postBindFixup();
                    if (lastAnd != null)
                        ((AndNode) lastAnd).setRightOperand(temp);
        
                    lastAnd = temp;
                    if (andNode == null)
                        andNode = temp;
                }
            }
            return (andNode == null) ? equal : andNode;
		}
		else
		{
			return this;
		}
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
		AndNode						 newAnd = null;
		BinaryComparisonOperatorNode leftBCO;
		BinaryComparisonOperatorNode rightBCO;
		int							 listSize = rightOperandList.size();
		ValueNode					 leftSide;

		if (SanityManager.DEBUG)
		SanityManager.ASSERT(listSize > 0,
			"rightOperandList.size() is expected to be > 0");

		if (! underNotNode)
		{
			return this;
		}
		
		if (!singleLeftOperand)
            throw StandardException.newException(SQLState.LANG_UNKNOWN);

		/* we want to convert the IN List into = OR = ... as
		 * described below.  
		 */

		/* Convert:
		 *		leftO IN rightOList.elementAt(0) , rightOList.elementAt(1) ...
		 * to:
		 *		leftO <> rightOList.elementAt(0) AND leftO <> rightOList.elementAt(1) ...
		 * NOTE - We do the conversion here since the single table clauses
		 * can be pushed down and the optimizer may eventually have a filter factor
		 * for <>.
		 */

		/* leftO <> rightOList.at(0) */
		/* If leftOperand is a ColumnReference, it may be remapped during optimization, and that
		 * requires each <> node to have a separate object.
		 */
		ValueNode leftClone = (getLeftOperand() instanceof ColumnReference) ? getLeftOperand().getClone() : getLeftOperand();
		leftBCO = (BinaryComparisonOperatorNode) 
					getNodeFactory().getNode(
						C_NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE,
						leftClone,
						(ValueNode) rightOperandList.elementAt(0),
						getContextManager());
		/* Set type info for the operator node */
		leftBCO.bindComparisonOperator();

		leftSide = leftBCO;

		for (int elemsDone = 1; elemsDone < listSize; elemsDone++)
		{

			/* leftO <> rightOList.elementAt(elemsDone) */
			leftClone = (getLeftOperand() instanceof ColumnReference) ? getLeftOperand().getClone() : getLeftOperand();
			rightBCO = (BinaryComparisonOperatorNode) 
						getNodeFactory().getNode(
							C_NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE,
							leftClone,
							(ValueNode) rightOperandList.elementAt(elemsDone),
							getContextManager());
			/* Set type info for the operator node */
			rightBCO.bindComparisonOperator();

			/* Create the AND */
			newAnd = (AndNode) getNodeFactory().getNode(
												C_NodeTypes.AND_NODE,
												leftSide,
												rightBCO,
												getContextManager());
			newAnd.postBindFixup();

			leftSide = newAnd;
		}

		return leftSide;
	}

	/**
	 * See if this IN list operator is referencing the same table.
	 *
	 * @param cr	The column reference.
	 *
	 * @return	true if in list references the same table as in cr.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean selfReference(ColumnReference cr)
		throws StandardException
	{
		int size = rightOperandList.size();
		for (int i = 0; i < size; i++)
		{
			ValueNode vn = (ValueNode) rightOperandList.elementAt(i);
			if (vn.getTablesReferenced().get(cr.getTableNumber()))
				return true;
		}
		return false;
	}

	/**
	 * The selectivity for an "IN" predicate is generally very small.
	 * This is an estimate applicable when in list are not all constants.
	 */
	public double selectivity(Optimizable optTable)
	{
		return 0.3d;
	}
 
	/**
	 * Do code generation for this IN list operator.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb The MethodBuilder the expression will go into
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException
	{
		int			listSize = rightOperandList.size();
		String		resultTypeName;
		String		receiverType = ClassName.DataValueDescriptor;
	
		String		leftInterfaceType = ClassName.DataValueDescriptor;
		String		rightInterfaceType = ClassName.DataValueDescriptor + "[]";

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(listSize > 0,
				"listSize is expected to be > 0");
		}

		/*
		** There are 2 parts to the code generation for an IN list -
		** the code in the constructor and the code for the expression evaluation.
		** The code that gets generated for the constructor is:
		**		DataValueDescriptor[] field = new DataValueDescriptor[size];
		**	For each element in the IN list that is a constant, we also generate:
		**		field[i] = rightOperandList[i];
		**	
		** If the IN list is composed entirely of constants, then we generate the
		** the following:
		**		leftOperand.in(rightOperandList, leftOperand, isNullable(), ordered, result);
		**
		** Otherwise, we create a new method.  This method contains the 
		** assignment of the non-constant elements into the array and the call to the in()
		** method, which is in the new method's return statement.  We then return a call
		** to the new method.
		*/

		/* Figure out the result type name */
		resultTypeName = getTypeCompiler().interfaceName();

		// Generate the code to build the array
		LocalField arrayField = generateListAsArray(acb, mb);

		/*
		** Call the method for this operator.
		*/
		/*
		** Generate (field = <left expression>).  This assignment is
		** used as the receiver of the method call for this operator,
		** and the field is used as the left operand:
		**
		**	(field = <left expression>).method(field, <right expression>...)
		*/

		//LocalField receiverField =
		//	acb.newFieldDeclaration(Modifier.PRIVATE, receiverType);

        if (leftOperandList.size() == 1) {
            getLeftOperand().generateExpression(acb, mb);
        }
        else {
            // Build a new ListDataType
            LocalField vals = PredicateList.generateListData(acb,
                                                             leftOperandList.size());
    
            for (int i = 0; i < leftOperandList.size(); i++) {
    
                ValueNode colRef = (ValueNode)leftOperandList.elementAt(i);
                
                // Call instance method setFrom to populate an entry in
                // the new ListDataType node with a data value from the
                // ValueNodeList (in this case these are column references).
                mb.getField(vals);
                colRef.generateExpression(acb, mb);
                mb.upCast(ClassName.DataValueDescriptor);
                // Populate the "i"th value.
                mb.push(i);
    
                mb.callMethod(VMOpcode.INVOKEVIRTUAL,
                    ClassName.ListDataType,
                    "setFrom",
                    "void", 2);
            }
            // Push the ListDataType on the stack as a DataValueDescriptor.
            mb.getField(vals);
            mb.upCast(ClassName.DataValueDescriptor);
            
        }
		mb.dup();
		//mb.putField(receiverField); // instance for method call
		/*mb.getField(receiverField);*/ mb.upCast(leftInterfaceType); // first arg
		mb.getField(arrayField); // second arg
		mb.push(isOrdered); // third arg
		mb.callMethod(VMOpcode.INVOKEINTERFACE, receiverType, methodName, resultTypeName, 3);
	}

	// It is expected that a DVD to move into the array has been pushed to the stack
	// before this method is called.
	protected void setDVDItemInArray(MethodBuilder mb, LocalField arrayField, int index,
								     int constIdx, int numValsInSet) throws StandardException {
		if (mb != null) {
			if (numValsInSet > 1) {
				// Push the ListDataType instance in prep for calling setFrom.
				mb.getField(arrayField);
				mb.getArrayElement(index);
				mb.cast(ClassName.ListDataType);

				mb.swap();
				mb.push(constIdx);

				mb.callMethod(VMOpcode.INVOKEVIRTUAL,
						(String) null,
						"setFrom",
						"void", 2);
			} else {
				// Push the DVD array field in preparation of calling setArrayElement.
				mb.getField(arrayField);
				mb.swap();
				mb.setArrayElement(index);
			}
		}
	}
	
	protected void buildDVDInArray(MethodBuilder mb, LocalField arrayField, int index,
								   LocalField rowArray, int numValsInSet) throws StandardException {
		if (mb != null) {
			if (numValsInSet > 1) {
				// Push the ListDataType instance in prep for calling setFrom.
				mb.getField(arrayField);
				mb.getArrayElement(index);
				mb.cast(ClassName.ListDataType);
				
				// Push the row as argument to setFrom.
				mb.getField(rowArray);
				mb.getArrayElement(index);
				mb.upCast(ClassName.ExecRow);
				
				mb.callMethod(VMOpcode.INVOKEVIRTUAL,
					(String) null,
					"setFrom",
					"void", 1);
			} else {
				// Push the DVD array field in preparation of calling setArrayElement.
				mb.getField(arrayField);
				
				// Push the ExecRow in preparation of calling getColumn.
				mb.getField(rowArray);
				mb.getArrayElement(index);
				mb.cast(ClassName.ValueRow);
				mb.push(1);
				mb.callMethod(VMOpcode.INVOKEVIRTUAL,
					(String) null,
					"getColumn",
					ClassName.DataValueDescriptor, 1);
				mb.setArrayElement(index);
			}
		}
	}
	
    /**
	 * Generate the code to create an array of DataValueDescriptors that
	 * will hold the IN-list values at execution time.  The array gets
	 * created in the constructor.  All constant elements in the array
	 * are initialized in the constructor.  All non-constant elements,
	 * if any, are initialized each time the IN list is evaluated.
	 *
	 * @param acb The ExpressionClassBuilder for the class we're generating
	 * @param mb The MethodBuilder the expression will go into
	 */
	protected LocalField generateListAsArray(ExpressionClassBuilder acb,
		MethodBuilder mb) throws StandardException
	{
		int listSize = rightOperandList.size();
		LocalField arrayField = acb.newFieldDeclaration(
			Modifier.PRIVATE, ClassName.DataValueDescriptor + "[]");

		/* Assign the initializer to the DataValueDescriptor[] field */
		MethodBuilder cb = acb.getConstructor();
		cb.pushNewArray(ClassName.DataValueDescriptor, listSize);
		cb.setField(arrayField);

        // Count the number of "constants" in each group.
        ValueNode constants = (ValueNode)rightOperandList.elementAt(0);
        int numValsInSet = constants instanceof ListValueNode ?
            ((ListValueNode) constants).numValues() : 1;
        
		/* Set the array elements that are constant */
        MethodBuilder nonConstantMethod = null;
		MethodBuilder currentConstMethod = cb;

		for (int index = 0; index < listSize; index++)
		{
            ValueNode topLevelLiteral = (ValueNode) rightOperandList.elementAt(index);
            ValueNode dataLiteral;
            MethodBuilder setArrayMethod = null;
			int numConstants = 0;
			int numNonConstants = 0;

            for (int constIdx = 0; constIdx < numValsInSet; constIdx++) {

                if (topLevelLiteral instanceof ListValueNode)
                    dataLiteral = ((ListValueNode) topLevelLiteral).getValue(constIdx);
                else
                    dataLiteral = topLevelLiteral;
    
                if (dataLiteral instanceof ConstantNode) {
                    numConstants++;
        
                    /*if too many statements are added  to a  method,
                     *size of method can hit  65k limit, which will
                     *lead to the class format errors at load time.
                     *To avoid this problem, when number of statements added
                     *to a method is > 2048, remaing statements are added to  a new function
                     *and called from the function which created the function.
                     *See Beetle 5135 or 4293 for further details on this type of problem.
                     */
                    if (constIdx == 0 &&
						currentConstMethod.statementNumHitLimit(numConstants)) {
                        MethodBuilder genConstantMethod = acb.newGeneratedFun("void", Modifier.PRIVATE);
                        currentConstMethod.pushThis();
                        currentConstMethod.callMethod(VMOpcode.INVOKEVIRTUAL,
                            (String) null,
                            genConstantMethod.getName(),
                            "void", 0);
                        //if it is a generate function, close the metod.
                        if (currentConstMethod != cb) {
                            currentConstMethod.methodReturn();
                            currentConstMethod.complete();
                        }
                        currentConstMethod = genConstantMethod;
                    }
                    setArrayMethod = currentConstMethod;
                } else {
					numNonConstants++;
                    if (nonConstantMethod == null)
                        nonConstantMethod = acb.newGeneratedFun("void", Modifier.PROTECTED);
                    setArrayMethod = nonConstantMethod;
        
                }
                if (constIdx == 0) {
                    if (numValsInSet > 1) {
						// Push the DVD array reference on the stack
						cb.getField(arrayField);
	
						// Build a new ListDataType, and place on the stack.
						PredicateList.generateListDataOnStack(acb, numValsInSet);
						cb.upCast(ClassName.DataValueDescriptor);
	
						// Store the list data in the DVD array.
						cb.setArrayElement(index);
					}
                }

				// Build the DVD to add to the DVD array, pushing it to the stack
				// cast as a DataValueDescriptor.
				dataLiteral.generateExpression(acb, setArrayMethod);
				setArrayMethod.upCast(ClassName.DataValueDescriptor);

				// Move the built DVD into the DVD array, using the proper
				// constant or non-constant method.
				setDVDItemInArray(setArrayMethod, arrayField, index, constIdx, numValsInSet);
            }
		}

		//if a generated function was created to reduce the size of the methods close the functions.
		if(currentConstMethod != cb){
			currentConstMethod.methodReturn();
			currentConstMethod.complete();
		}

		if (nonConstantMethod != null) {
			nonConstantMethod.methodReturn();
			nonConstantMethod.complete();
			mb.pushThis();
			mb.callMethod(VMOpcode.INVOKEVIRTUAL, (String) null, nonConstantMethod.getName(), "void", 0);
		}

		return arrayField;
	}


	/**
	 * Generate start/stop key for this IN list operator.  Bug 3858.
	 *
	 * @param isAsc		is the index ascending on the column in question
	 * @param isStartKey	are we generating start key or not
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb The MethodBuilder the expression will go into
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generateStartStopKey(boolean isAsc, boolean isStartKey,
									 ExpressionClassBuilder acb,
									 MethodBuilder mb)
											   throws StandardException
	{
		/* left side of the "in" operator is our "judge" when we try to get
		 * the min/max value of the operands on the right side.  Judge's type
		 * is important for us, and is input parameter to min/maxValue.
		 */
		// TODO-msirek: Handle multiple start/stop keys for multicolumn in list.
		if (leftOperandList.size() > 1)
			SanityManager.THROWASSERT("Not expecting multiple start/stop keys on multicolumn in list.");
        for (int listIdx = 0; listIdx < leftOperandList.size(); listIdx++) {
            ValueNode leftOperand = (ValueNode) leftOperandList.elementAt(listIdx);
            int leftTypeFormatId = leftOperand.getTypeId().getTypeFormatId();
            int leftJDBCTypeId = leftOperand.getTypeId().isUserDefinedTypeId() ?
                leftOperand.getTypeId().getJDBCTypeId() : -1;
    
            int listSize = rightOperandList.size();
            int numLoops, numValInLastLoop, currentOpnd = 0;
    
            /* We first calculate how many times (loops) we generate a call to
             * min/maxValue function accumulatively, since each time it at most
             * takes 4 input values.  An example of the calls generated will be:
             * minVal(minVal(...minVal(minVal(v1,v2,v3,v4,judge), v5,v6,v7,judge),
             *        ...), vn-1, vn, NULL, judge)
             * Unused value parameters in the last call are filled with NULLs.
             */
            if (listSize < 5) {
                numLoops = 1;
                numValInLastLoop = (listSize - 1) % 4 + 1;
            } else {
                numLoops = (listSize - 5) / 3 + 2;
                numValInLastLoop = (listSize - 5) % 3 + 1;
            }
    
            for (int i = 0; i < numLoops; i++) {
                /* generate value parameters of min/maxValue
                 */
                int numVals = (i == numLoops - 1) ? numValInLastLoop :
                    ((i == 0) ? 4 : 3);
                for (int j = 0; j < numVals; j++) {
                    ValueNode vn = (ValueNode) rightOperandList.elementAt(currentOpnd++);
                    //if (vn instanceof ListValueNode)
                    //    vn = ((ListValueNode)vn).getValue(listIdx);
                    vn.generateExpression(acb, mb);
                    mb.upCast(ClassName.DataValueDescriptor);
                }
        
                /* since we have fixed number of input values (4), unused ones
                 * in the last loop are filled with NULLs
                 */
                int numNulls = (i < numLoops - 1) ? 0 :
                    ((i == 0) ? 4 - numValInLastLoop : 3 - numValInLastLoop);
                for (int j = 0; j < numNulls; j++)
                    mb.pushNull(ClassName.DataValueDescriptor);
        
                /* have to put judge's types in the end
                 */
                mb.push(leftTypeFormatId);
                mb.push(leftJDBCTypeId);
        
                /* decide to get min or max value
                 */
                String methodName;
                if ((isAsc && isStartKey) || (!isAsc && !isStartKey))
                    methodName = "minValue";
                else
                    methodName = "maxValue";
        
                mb.callMethod(VMOpcode.INVOKESTATIC, ClassName.BaseExpressionActivation, methodName, ClassName.DataValueDescriptor, 6);
        
            }
        }
	}
	
	/**
	 * Indicate that the IN-list values for this node are ordered (i.e. they
	 * are all constants and they have been sorted).
	 */
	protected void markAsOrdered(boolean ordered)
	{
		isOrdered = ordered;
	}

	/**
	 * Indicate that the IN-list values for this node must be sorted
	 * in DESCENDING order.  This only applies to in-list "multi-probing",
	 * where the rows are processed in the order of the IN list elements
	 * themselves.  In that case, any requirement to sort the rows in
	 * descending order means that the values in the IN list have to
	 * be sorted in descending order, as well.
	 */
	protected void markSortDescending()
	{
		sortDescending = true;
	}

	/**
	 * Return whether or not the IN-list values for this node are ordered.
	 * This is used for determining whether or not we need to do an execution-
	 * time sort.
	 */
	protected boolean isOrdered()
	{
		return isOrdered;
	} 

	/**
	 * Return whether or not the IN-list values for this node must be
	 * sorted in DESCENDING order.
	 */
	protected boolean sortDescending()
	{
		return sortDescending;
	}
	
	@Override
	public int hashCode(){
		int result=(isOrdered?1:0);
		result=31*result+(sortDescending?1:0);
		result = 31*result + leftOperandList.hashCode();
		result = 31*result + rightOperandList.hashCode();
		return result;
	}
}
