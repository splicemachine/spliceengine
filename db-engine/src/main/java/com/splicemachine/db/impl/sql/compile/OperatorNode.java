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

import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.SqlXmlUtil;
import com.splicemachine.db.iapi.types.TypeId;

/**
 * Abstract base-class for the various operator nodes: UnaryOperatorNode,
 * BinaryOperatorNode and TernaryOperatorNode.
 */
public abstract class OperatorNode extends ValueNode
{
    String    operator;
    String    methodName;

    List<ValueNode> operands;
    List<String> interfaceTypes;

    String resultInterfaceType;

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
            if (operands.get(i) != null) {
                operands.set(i, operands.get(i).bindExpression(fromList, subqueryList, aggregateVector));
            }
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
            if (operands.get(i) != null) {
                operands.set(i, operands.get(i).preprocess(
                        numTables,
                        outerFromList, outerSubqueryList,
                        outerPredicateList));
            }
        }
        return this;
    }

    @Override
    public boolean checkCRLevel(int level){
        return operands.stream().anyMatch(op -> op != null && op.checkCRLevel(level));
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
            if (operands.get(i) != null) {
                operands.set(i, operands.get(i).remapColumnReferencesToExpressions());
            }
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
        return operands.stream().allMatch(op -> op == null || op.isConstantExpression());
    }

    /** @see ValueNode#constantExpression */
    public boolean constantExpression(PredicateList whereClause)
    {
        return operands.stream().allMatch(op -> op == null || op.constantExpression(whereClause));
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
    @Override
    protected int getOrderableVariantType() throws StandardException {
        int min = Qualifier.CONSTANT;
        for (ValueNode op : operands) {
            if (op != null) {
                min = Math.min(min, op.getOrderableVariantType());
            }
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

    private boolean isSignatureEquivalent(ValueNode o) throws StandardException {
        if (!isSameNodeType(o))
            return false;
        OperatorNode other = (OperatorNode)o;
        if (methodName == null ^ other.methodName == null)
            return false;
        if (methodName != null && !methodName.equals(other.methodName))
            return false;
        if (operator == null ^ other.operator == null)
            return false;
        if (operator != null && !operator.equals(other.operator))
            return false;
        return operands.size() == other.operands.size();
    }

    /**
     * @inheritDoc
     */
    protected boolean isEquivalent(ValueNode o) throws StandardException {
        if (!isSignatureEquivalent(o))
            return false;
        OperatorNode other = (OperatorNode)o;
        for (int i = 0 ; i < operands.size(); ++i) {
            if (operands.get(i) == null ^ other.operands.get(i) == null)
                return false;
            if (operands.get(i) != null && !operands.get(i).isEquivalent(other.operands.get(i)))
                return false;
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isSemanticallyEquivalent(ValueNode o) throws StandardException {
        if (!isSignatureEquivalent(o))
            return false;
        OperatorNode other = (OperatorNode)o;
        for (int i = 0 ; i < operands.size(); ++i) {
            if (operands.get(i) == null ^ other.operands.get(i) == null)
                return false;
            if (operands.get(i) != null && !operands.get(i).isSemanticallyEquivalent(other.operands.get(i)))
                return false;
        }
        return true;
    }


    public int hashCode() {
        int hashCode = getBaseHashCode();
        hashCode = 31 * hashCode + methodName.hashCode();
        for (ValueNode operand: operands) {
            hashCode = 31 * hashCode + (operand == null ? 0 : operand.hashCode());
        }
        return hashCode;
    }

    public List<? extends QueryTreeNode> getChildren() {
        return operands.stream().filter(Objects::nonNull).collect(Collectors.toList());
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
            if (op != null) {
                cardinality = Math.max(cardinality, op.nonZeroCardinality(numberOfRows));
            }
        }
        return cardinality;
    }

    @Override
    public boolean isConstantOrParameterTreeNode() {
        return operands.stream().allMatch(op -> op == null || op.isConstantOrParameterTreeNode());
    }

    @Override
    public double getBaseOperationCost() throws StandardException {
        double cost = 0.0;
        for (ValueNode op : operands) {
            cost += op == null ? 0.0 : op.getBaseOperationCost();
        }
        return cost;
    }


    /**
     * <p>
     * Generate code that pushes an SqlXmlUtil instance onto the stack. The
     * instance will be created and cached in the activation's constructor, so
     * that we don't need to create a new instance for every row.
     * </p>
     *
     * <p>
     * If the {@code xmlQuery} parameter is non-null, there will also be code
     * that compiles the query when the SqlXmlUtil instance is created.
     * </p>
     *
     * @param acb builder for the class in which the generated code lives
     * @param mb builder for the method that implements this operator
     * @param xmlQuery the XML query to be executed by the operator, or
     * {@code null} if this isn't an XMLEXISTS or XMLQUERY operator
     * @param xmlOpName the name of the operator (ignored if {@code xmlQuery}
     * is {@code null})
     */
    static void pushSqlXmlUtil(
            ExpressionClassBuilder acb, MethodBuilder mb,
            String xmlQuery, String xmlOpName) {

        // Create a field in which the instance can be cached.
        LocalField sqlXmlUtil = acb.newFieldDeclaration(
                Modifier.PRIVATE | Modifier.FINAL, SqlXmlUtil.class.getName());

        // Add code that creates the SqlXmlUtil instance in the constructor.
        MethodBuilder constructor = acb.getConstructor();
        constructor.pushNewStart(SqlXmlUtil.class.getName());
        constructor.pushNewComplete(0);
        constructor.putField(sqlXmlUtil);

        // Compile the query, if one is specified.
        if (xmlQuery == null) {
            // No query. The SqlXmlUtil instance is still on the stack. Pop it
            // to restore the initial state of the stack.
            constructor.pop();
        } else {
            // Compile the query. This will consume the SqlXmlUtil instance
            // and leave the stack in its initial state.
            constructor.push(xmlQuery);
            constructor.push(xmlOpName);
            constructor.callMethod(
                    VMOpcode.INVOKEVIRTUAL, SqlXmlUtil.class.getName(),
                    "compileXQExpr", "void", 2);
        }

        // Read the cached value and push it onto the stack in the method
        // generated for the operator.
        mb.getField(sqlXmlUtil);
    }

    @Override
    public int getTableNumber() {
        return operands.stream()
                .filter(op -> op != null && op.getTableNumber() != -1)
                .findFirst()
                .map(ValueNode::getTableNumber)
                .orElse(-1);
    }
}
