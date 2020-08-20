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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.AggregateDefinition;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import splice.com.google.common.collect.Lists;

import static com.splicemachine.db.iapi.sql.compile.AggregateDefinition.fromString;

/**
 * @author Jeff Cunningham
 * Date: 10/2/14
 */
public class WrappedAggregateFunctionNode extends WindowFunctionNode {
    private AggregateNode aggregateFunction;

    /**
     * Initializer. QueryTreeNode override.
     *
     * @param arg1 the window definition
     * @param arg2 the wrapped aggregate function
     * @throws com.splicemachine.db.iapi.error.StandardException
     */
    public void init(Object arg1, Object arg2) throws StandardException {
        super.init(arg1, null);
        aggregateFunction = (AggregateNode) arg2;
        this.aggregateName = aggregateFunction.aggregateName;
        this.type = fromString(aggregateFunction.aggregateName);
        this.operator = aggregateFunction.operator;
    }

    @Override
    public String getName() {
        return aggregateFunction.getAggregateName();
    }

    @Override
    public ValueNode getNewNullResultExpression() throws StandardException {
        return aggregateFunction.getNewNullResultExpression();
    }

    @Override
    public DataTypeDescriptor getTypeServices() {
        return aggregateFunction.getTypeServices();
    }

    @Override
    public List<ValueNode> getOperands() {
        ValueNode wrappedOperand = aggregateFunction.operand;
        return (wrappedOperand != null ? Lists.newArrayList(wrappedOperand) : Collections.EMPTY_LIST);
    }

    @Override
    public void replaceOperand(ValueNode oldVal, ValueNode newVal) {
        // TODO: JC - what else? bind?
        aggregateFunction.operand = newVal;
    }

    /**
     * Use in visitor pattern to process children
     * Visit the underlying aggregate function.
     *
     * @param v
     * @throws StandardException
     */

    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        if (aggregateFunction != null) {
            aggregateFunction.accept(v, this);
        }
    }

    /**
     * Overridden to redirect the call to the wrapped aggregate node.
     *
     * @param tableNumber  The tableNumber for the new ColumnReference
     * @param nestingLevel this node's nesting level
     * @return the new CR
     * @throws StandardException
     */
    @Override
    public ValueNode replaceCallWithColumnReference(int tableNumber, int nestingLevel) throws StandardException {
        ColumnReference node = (ColumnReference) aggregateFunction.replaceAggregatesWithColumnReferences(
                (ResultColumnList) getNodeFactory().getNode(
                        C_NodeTypes.RESULT_COLUMN_LIST,
                        getContextManager()),
                tableNumber);

        // Mark the ColumnReference as being generated to replace a call to
        // a window function
        node.markGeneratedToReplaceWindowFunctionCall();
        return node;
    }

    /**
     * QueryTreeNode override. Prints the sub-nodes of this object.
     *
     * @param depth The depth of this node in the tree
     * @see QueryTreeNode#printSubNodes
     */
    public void printSubNodes(int depth) {
        if (SanityManager.DEBUG) {
            super.printSubNodes(depth);

            printLabel(depth, "aggregate: ");
            aggregateFunction.treePrint(depth + 1);
        }
    }

    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException {
        // DB-2086 - Vector.remove() calls node1.isEquivalent(node2), not node1.equals(node2), which
        // returns true for identical aggregate nodes removing the first aggregate, not necessarily
        // this one. We need to create a tmp Vector and add all Agg nodes found but this delegate by
        // checking for object identity (==)
        List<AggregateNode> tmp = new ArrayList<>();
        aggregateFunction.bindExpression(fromList, subqueryList, tmp);

        // We don't want to be in this aggregateVector - we add all aggs found during bind except
        // this delegate.
        // We want to bind the wrapped aggregate (and operand, etc) but we don't want to show up
        // in this list as an aggregate. The list will be handed to GroupByNode, which we don't
        // want doing the work.  Window function code will handle the window function aggregates
        for (AggregateNode aggFn : tmp) {
            if (aggregateFunction != aggFn) {
                aggregateVector.add(aggFn);
            }
        }

        // Now that delegate is bound, set some required fields on this
        // TODO: JC What all is required?
        this.operator = aggregateFunction.operator;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        WrappedAggregateFunctionNode that = (WrappedAggregateFunctionNode) o;

        return aggregateFunction.equals(that.aggregateFunction);

    }

    @Override
    public int hashCode() {
        return aggregateFunction.hashCode();
    }

    /**
     * Get the generated ResultColumn where this
     * aggregate now resides after a call to
     * replaceAggregatesWithColumnReference().
     *
     * @return the result column
     */
    @Override
    AggregateDefinition getAggregateDefinition() {
        return aggregateFunction.getAggregateDefinition();
    }

    @Override
    public boolean isDistinct() {
        return aggregateFunction.isDistinct();
    }

    @Override
    public String getAggregatorClassName() {
        return aggregateFunction.getAggregatorClassName();
    }

    @Override
    public String getAggregateName() {
        return aggregateFunction.getAggregateName();
    }

    @Override
    public ResultColumn getNewAggregatorResultColumn(DataDictionary dd) throws StandardException {
        return aggregateFunction.getNewAggregatorResultColumn(dd);
    }

    @Override
    public ResultColumn getNewExpressionResultColumn(DataDictionary dd) throws StandardException {
        return aggregateFunction.getNewExpressionResultColumn(dd);
    }

    @Override
    public void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb) throws StandardException {
        aggregateFunction.generateExpression(acb, mb);
    }

    @Override
    public String getSQLName() {
        return aggregateFunction.getSQLName();
    }

    @Override
    public ColumnReference getGeneratedRef() {
        return aggregateFunction.getGeneratedRef();
    }

    @Override
    public ResultColumn getGeneratedRC() {
        return aggregateFunction.getGeneratedRC();
    }

    public FormatableHashtable getFunctionSpecificArgs() {
        FormatableHashtable ht = new FormatableHashtable();
        if (aggregateFunction instanceof StringAggregateNode)
            ht.put("param", ((StringAggregateNode)aggregateFunction).getParameter());
        return ht;
    }
}
