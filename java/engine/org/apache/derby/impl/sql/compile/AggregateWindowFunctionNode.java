/*
   Derby - Class org.apache.derby.impl.sql.compile.AggregateWindowFunctionNode

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.compile;

import java.util.Vector;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

/**
 * Represents aggregate function calls on a window. Delegates to real
 * aggregate function.
 */
public final class AggregateWindowFunctionNode extends WindowFunctionNode {

    private AggregateNode aggregateFunction;

    /**
     * Initializer. QueryTreeNode override.
     *
     * @param arg1 The window definition or reference
     * @param arg2 aggregate function node
     * @throws StandardException
     */
    public void init(Object arg1, Object arg2) throws StandardException {
        super.init(null, "?", arg1);
        aggregateFunction = (AggregateNode) arg2;
    }

    @Override
    public ValueNode getNewNullResultExpression() throws StandardException {
        return aggregateFunction.getNewNullResultExpression();
    }

    @Override
    public AggregateNode getWrappedAggregate() {
        return this.aggregateFunction;
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
    public ValueNode replaceAggregatesWithColumnReferences(ResultColumnList rcl,
                                                           int tableNumber) throws StandardException {
        return aggregateFunction.replaceAggregatesWithColumnReferences(rcl,tableNumber);
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
    public ValueNode bindExpression(FromList fromList, SubqueryList subqueryList, Vector aggregateVector) throws
        StandardException {
        return aggregateFunction.bindExpression(fromList, subqueryList, aggregateVector);
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
}
