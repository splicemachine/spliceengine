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
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.types.TypeId;

import java.util.*;
import java.util.stream.IntStream;

/**
 * A BinaryOperatorNode represents a built-in binary operator as defined by
 * the ANSI/ISO SQL standard.  This covers operators like +, -, *, /, =, <, etc.
 * Java operators are not represented here: the JSQL language allows Java
 * methods to be called from expressions, but not Java operators.
 *
 */

public class GenericOperatorNode extends OperatorNode
{
    String    operator;
    String    methodName;

    List<ValueNode> operands;
    List<String> interfaceTypes;

    String resultInterfaceType;

    public void init(List<ValueNode> operands) {
        this.operands = operands;
        this.interfaceTypes = new ArrayList<>(operands.size());
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return    This object as a String
     */

    public String toString()
    {
        if (SanityManager.DEBUG)
        {
            return "operator: " + operator + "\n" +
                    "methodName: " + methodName + "\n" +
                    super.toString();
        }
        else
        {
            return "";
        }
    }

    public String getOperatorString(){
        return operator;
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth        The depth of this node in the tree
     */

    public void printSubNodes(int depth)
    {
        if (SanityManager.DEBUG)
        {
            super.printSubNodes(depth);
            for (int i = 0; i < operands.size(); ++i) {
                ValueNode op = operands.get(i);
                if (op != null) {
                    printLabel(depth, "operand " + i + "");
                    op.treePrint(depth + 1);
                }
            }
        }
    }

    public void bindOperands(FromList fromList,
                             SubqueryList subqueryList,
                             List<AggregateNode>    aggregateVector) throws StandardException
    {
        for (int i = 0; i < operands.size(); ++i) {
            operands.set(i, operands.get(i).bindExpression(fromList, subqueryList, aggregateVector));
        }
    }


    /** generate a SQL->Java->SQL conversion tree above the
     * operands of this Generic Operator Node if needed. Subclasses can override
     * the default behavior.
     */
    public ValueNode genSQLJavaSQLTree() throws StandardException
    {
        for (int i = 0; i < operands.size(); ++i) {
            TypeId typeId = operands.get(i).getTypeId();
            if (typeId.userType())
                operands.set(i, operands.get(i).genSQLJavaSQLTree());
        }

        return this;
    }

    /**
     * Preprocess an expression tree.  We do a number of transformations
     * here (including subqueries, IN lists, LIKE and BETWEEN) plus
     * subquery flattening.
     * NOTE: This is done before the outer ResultSetNode is preprocessed.
     *
     * @param    numTables            Number of tables in the DML Statement
     * @param    outerFromList        FromList from outer query block
     * @param    outerSubqueryList    SubqueryList from outer query block
     * @param    outerPredicateList    PredicateList from outer query block
     *
     * @return        The modified expression
     *
     * @exception StandardException        Thrown on error
     */
    public ValueNode preprocess(int numTables,
                                FromList outerFromList,
                                SubqueryList outerSubqueryList,
                                PredicateList outerPredicateList)
            throws StandardException
    {
        for (int i = 0; i < operands.size(); ++i) {
            operands.set(i, operands.get(i).preprocess(
                    numTables,
                    outerFromList, outerSubqueryList,
                    outerPredicateList));
        }
        return this;
    }

    @Override
    public boolean checkCRLevel(int level){
        return operands.stream().anyMatch(op -> op.checkCRLevel(level));
    }


    /**
     * Remap all ColumnReferences in this tree to be clones of the
     * underlying expression.
     *
     * @return ValueNode            The remapped expression tree.
     *
     * @exception StandardException            Thrown on error
     */
    public ValueNode remapColumnReferencesToExpressions()
            throws StandardException
    {
        for (int i = 0; i < operands.size(); ++i) {
            operands.set(i, operands.get(i).remapColumnReferencesToExpressions());
        }
        return this;
    }

    /**
     * Return whether or not this expression tree represents a constant expression.
     *
     * @return    Whether or not this expression tree represents a constant expression.
     */
    public boolean isConstantExpression()
    {
        return operands.stream().allMatch(ValueNode::isConstantExpression);
    }

    /** @see ValueNode#constantExpression */
    public boolean constantExpression(PredicateList whereClause)
    {
        return operands.stream().allMatch(op -> op.constantExpression(whereClause));
    }


    /**
     * Return the variant type for the underlying expression.
     * The variant type can be:
     *        VARIANT                - variant within a scan
     *                              (method calls and non-static field access)
     *        SCAN_INVARIANT        - invariant within a scan
     *                              (column references from outer tables)
     *        QUERY_INVARIANT        - invariant within the life of a query
     *        CONSTANT            - immutable
     *
     * @return    The variant type for the underlying expression.
     * @exception StandardException    thrown on error
     */
    protected int getOrderableVariantType() throws StandardException {
        int min = Integer.MAX_VALUE;
        for (ValueNode op : operands) {
            min = Math.min(min, op.getOrderableVariantType());
        }
        return min;
    }

    /**
     * Accept the visitor for all visitable children of this node.
     *
     * @param v the visitor
     */
    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        for (int i = 0; i < operands.size(); ++i) {
            if (operands.get(i) != null) {
                operands.set(i, (ValueNode) operands.get(i).accept(v, this));
            }
        }
    }

    /**
     * @inheritDoc
     */
    protected boolean isEquivalent(ValueNode o) throws StandardException {
        if (!isSameNodeType(o))
            return false;
        GenericOperatorNode other = (GenericOperatorNode)o;
        if (!methodName.equals(other.methodName))
            return false;
        if (operands.size() != other.operands.size())
            return false;
        for (int i = 0 ; i < operands.size(); ++i) {
            if (!operands.get(i).isEquivalent(other.operands.get(i)))
                return false;
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isSemanticallyEquivalent(ValueNode o) throws StandardException {
        if (!isSameNodeType(o)) {
            return false;
        }
        GenericOperatorNode other = (GenericOperatorNode)o;
        if (!methodName.equals(other.methodName))
            return false;
        if (operands.size() != other.operands.size())
            return false;
        for (int i = 0 ; i < operands.size(); ++i) {
            if (!operands.get(i).isSemanticallyEquivalent(other.operands.get(i)))
                return false;
        }
        return true;
    }


    public int hashCode() {
        int hashCode = getBaseHashCode();
        hashCode = 31 * hashCode + methodName.hashCode();
        for (ValueNode operand: operands) {
            hashCode = 31 * hashCode + operand.hashCode();
        }
        return hashCode;
    }

    public List<? extends QueryTreeNode> getChildren() {
        return operands;
    }

    @Override
    public QueryTreeNode getChild(int index) {
        return operands.get(index);
    }

    @Override
    public void setChild(int index, QueryTreeNode newValue) {
        assert newValue instanceof ValueNode;
        operands.set(index, (ValueNode) newValue);
    }

    @Override
    public long nonZeroCardinality(long numberOfRows) throws StandardException {
        long cardinality = 0;
        for (ValueNode op : operands) {
            cardinality = Math.max(cardinality, op.nonZeroCardinality(numberOfRows));
        }
        return cardinality;
    }

    @Override
    public int getTableNumber() {
        return operands.stream()
                .filter(op -> op.getTableNumber() != -1)
                .findFirst()
                .map(ValueNode::getTableNumber)
                .orElse(-1);
    }

    @Override
    public boolean isConstantOrParameterTreeNode() {
        return operands.stream().allMatch(ValueNode::isConstantOrParameterTreeNode);
    }

    @Override
    public double getBaseOperationCost() throws StandardException {
        double cost = 0.0;
        for (ValueNode op : operands) {
            cost += op == null ? 0.0 : op.getBaseOperationCost();
        }
        return cost;
    }
}

