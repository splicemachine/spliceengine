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
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.util.JBitSet;

import java.util.LinkedList;
import java.util.List;

/**
 * A BinaryListOperatorNode represents a built-in "binary" operator with a single
 * operand on the left of the operator and a list of operands on the right.
 * This covers operators such as IN and BETWEEN.
 */

public abstract class BinaryListOperatorNode extends ValueNode{
    String methodName;
    /* operator used for error messages */
    String operator;

    String leftInterfaceType;
    String rightInterfaceType;

    ValueNode receiver; // used in generation
    ValueNode leftOperand;
    ValueNodeList rightOperandList;

    /**
     * Initializer for a BinaryListOperatorNode
     *
     * @param leftOperand      The left operand of the node
     * @param rightOperandList The right operand list of the node
     * @param operator         String representation of operator
     */

    public void init(Object leftOperand,Object rightOperandList,
                     Object operator,Object methodName){
        this.leftOperand=(ValueNode)leftOperand;
        this.rightOperandList=(ValueNodeList)rightOperandList;
        this.operator=(String)operator;
        this.methodName=(String)methodName;
    }

    public String getOperator(){return operator;}
    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */
    @Override
    public String toString(){
        if(SanityManager.DEBUG){
            return "operator: "+operator+"\n"+
                    "methodName: "+methodName+"\n"+
                    super.toString();
        }else{
            return "";
        }
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */
    @Override
    public void printSubNodes(int depth){
        if(SanityManager.DEBUG){
            super.printSubNodes(depth);

            if(leftOperand!=null){
                printLabel(depth,"leftOperand: ");
                leftOperand.treePrint(depth+1);
            }

            if(rightOperandList!=null){
                printLabel(depth,"rightOperandList: ");
                rightOperandList.treePrint(depth+1);
            }
        }
    }

    /**
     * Bind this expression.  This means binding the sub-expressions,
     * as well as figuring out what the return type is for this expression.
     *
     * @param fromList        The FROM list for the query this
     *                        expression is in, for binding columns.
     * @param subqueryList    The subquery list being built as we find SubqueryNodes
     * @param aggregateVector The aggregate vector being built as we find AggregateNodes
     * @throws StandardException Thrown on error
     * @return The new top of the expression tree.
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException{
        leftOperand=leftOperand.bindExpression(fromList,subqueryList,aggregateVector);
        rightOperandList.bindExpression(fromList,subqueryList,aggregateVector);

		/* Is there a ? parameter on the left? */
        if(leftOperand.requiresTypeFromContext()){
            /*
			** It's an error if both operands are all ? parameters.
			*/
            if(rightOperandList.containsAllParameterNodes()){
                throw StandardException.newException(SQLState.LANG_BINARY_OPERANDS_BOTH_PARMS,
                        operator);
            }

			/* Set the left operand to the type of right parameter. */
            leftOperand.setType(rightOperandList.getTypeServices());
        }

		/* Is there a ? parameter on the right? */
        if(rightOperandList.containsParameterNode()){
			/* Set the right operand to the type of the left parameter. */
            rightOperandList.setParameterDescriptor(leftOperand.getTypeServices());
        }

		/* If the left operand is not a built-in type, then generate a conversion
		 * tree to a built-in type.
		 */
        if(leftOperand.getTypeId().userType()){
            leftOperand=leftOperand.genSQLJavaSQLTree();
        }

		/* Generate bound conversion trees for those elements in the rightOperandList
		 * that are not built-in types.
		 */
        rightOperandList.genSQLJavaSQLTrees();

		/* Test type compatability and set type info for this node */
        bindComparisonOperator();

        return this;
    }

    /**
     * Test the type compatability of the operands and set the type info
     * for this node.  This method is useful both during binding and
     * when we generate nodes within the language module outside of the parser.
     *
     * @throws StandardException Thrown on error
     */
    public void bindComparisonOperator() throws StandardException{
        boolean nullableResult;

		/* Can the types be compared to each other? */
        rightOperandList.comparable(leftOperand);

		/*
		** Set the result type of this comparison operator based on the
		** operands.  The result type is always SQLBoolean - the only question
		** is whether it is nullable or not.  If either the leftOperand or
		** any of the elements in the rightOperandList is
		** nullable, the result of the comparison must be nullable, too, so
		** we can represent the unknown truth value.
		*/
        nullableResult=leftOperand.getTypeServices().isNullable() ||
                rightOperandList.isNullable();
        setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID,nullableResult));
    }

    /**
     * Preprocess an expression tree.  We do a number of transformations
     * here (including subqueries, IN lists, LIKE and BETWEEN) plus
     * subquery flattening.
     * NOTE: This is done before the outer ResultSetNode is preprocessed.
     *
     * @throws StandardException Thrown on error
     * @param    numTables            Number of tables in the DML Statement
     * @param    outerFromList        FromList from outer query block
     * @param    outerSubqueryList    SubqueryList from outer query block
     * @param    outerPredicateList    PredicateList from outer query block
     * @return The modified expression
     */
    @Override
    public ValueNode preprocess(int numTables,
                                FromList outerFromList,
                                SubqueryList outerSubqueryList,
                                PredicateList outerPredicateList) throws StandardException{
        leftOperand=leftOperand.preprocess(numTables,
                outerFromList,outerSubqueryList,
                outerPredicateList);
        rightOperandList.preprocess(numTables,
                outerFromList,outerSubqueryList,
                outerPredicateList);
        return this;
    }

    /**
     * Set the leftOperand to the specified ValueNode
     *
     * @param newLeftOperand The new leftOperand
     */
    public void setLeftOperand(ValueNode newLeftOperand){
        leftOperand=newLeftOperand;
    }

    /**
     * Get the leftOperand
     *
     * @return The current leftOperand.
     */
    public ValueNode getLeftOperand(){
        return leftOperand;
    }

    /**
     * Set the rightOperandList to the specified ValueNodeList
     *
     * @param newRightOperandList The new rightOperandList
     */
    public void setRightOperandList(ValueNodeList newRightOperandList){
        rightOperandList=newRightOperandList;
    }

    /**
     * Get the rightOperandList
     *
     * @return The current rightOperandList.
     */
    public ValueNodeList getRightOperandList(){
        return rightOperandList;
    }

    /**
     * Categorize this predicate.  Initially, this means
     * building a bit map of the referenced tables for each predicate.
     * If the source of this ColumnReference (at the next underlying level)
     * is not a ColumnReference or a VirtualColumnNode then this predicate
     * will not be pushed down.
     * <p/>
     * For example, in:
     * select * from (select 1 from s) a (x) where x = 1
     * we will not push down x = 1.
     * NOTE: It would be easy to handle the case of a constant, but if the
     * inner SELECT returns an arbitrary expression, then we would have to copy
     * that tree into the pushed predicate, and that tree could contain
     * subqueries and method calls.
     * RESOLVE - revisit this issue once we have views.
     *
     * @param referencedTabs  JBitSet with bit map of referenced FromTables
     * @param simplePredsOnly Whether or not to consider method
     *                        calls, field references and conditional nodes
     *                        when building bit map
     * @return boolean        Whether or not source.expression is a ColumnReference
     * or a VirtualColumnNode.
     * @throws StandardException Thrown on error
     */
    @Override
    public boolean categorize(JBitSet referencedTabs,boolean simplePredsOnly) throws StandardException{
        boolean pushable;
        pushable=leftOperand.categorize(referencedTabs,simplePredsOnly);
        pushable=(rightOperandList.categorize(referencedTabs,simplePredsOnly) && pushable);
        return pushable;
    }

    /**
     * Remap all ColumnReferences in this tree to be clones of the
     * underlying expression.
     *
     * @return ValueNode            The remapped expression tree.
     * @throws StandardException Thrown on error
     */
    @Override
    public ValueNode remapColumnReferencesToExpressions() throws StandardException{
        // we need to assign back because a new object may be returned, beetle 4983
        leftOperand=leftOperand.remapColumnReferencesToExpressions();
        rightOperandList.remapColumnReferencesToExpressions();
        return this;
    }

    /**
     * Return whether or not this expression tree represents a constant expression.
     *
     * @return Whether or not this expression tree represents a constant expression.
     */
    @Override
    public boolean isConstantExpression(){
        return (leftOperand.isConstantExpression() && rightOperandList.isConstantExpression());
    }

    @Override
    public boolean constantExpression(PredicateList whereClause){
        return (leftOperand.constantExpression(whereClause) &&
                rightOperandList.constantExpression(whereClause));
    }

    /**
     * Return the variant type for the underlying expression.
     * The variant type can be:
     * VARIANT				- variant within a scan
     * (method calls and non-static field access)
     * SCAN_INVARIANT		- invariant within a scan
     * (column references from outer tables)
     * QUERY_INVARIANT		- invariant within the life of a query
     * CONSTANT			- immutable
     *
     * @throws StandardException thrown on error
     * @return The variant type for the underlying expression.
     */
    @Override
    protected int getOrderableVariantType() throws StandardException{
        int leftType=leftOperand.getOrderableVariantType();
        int rightType=rightOperandList.getOrderableVariantType();

        return Math.min(leftType,rightType);
    }

    @Override
    public void acceptChildren(Visitor v) throws StandardException{
        super.acceptChildren(v);

        if(leftOperand!=null){
            leftOperand=(ValueNode)leftOperand.accept(v, this);
        }

        if(rightOperandList!=null){
            rightOperandList=(ValueNodeList)rightOperandList.accept(v, this);
        }
    }

    @Override
    protected boolean isEquivalent(ValueNode o) throws StandardException{
        if(!isSameNodeType(o)){
            return false;
        }
        BinaryListOperatorNode other=(BinaryListOperatorNode)o;
        if(!operator.equals(other.operator) || !leftOperand.isEquivalent(other.getLeftOperand())){
            return false;
        }

        return rightOperandList.isEquivalent(other.rightOperandList);
    }

    public List getChildren(){
        return new LinkedList(){{
            add(leftOperand);
            addAll(rightOperandList.getNodes());
        }};
    }

}
