/*
   Derby - Class org.apache.derby.impl.sql.compile.RowNumberFunctionNode

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

package com.splicemachine.db.impl.sql.compile;

import java.util.List;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

/**
 * Class that represents a call to the LAST_VALUE() window function.
 */
public final class LastValueFunctionNode extends WindowFunctionNode {

    /**
     * Initializer. QueryTreeNode override.
     *
     * @param arg1 The function's definition class
     * @param arg2 The window definition or reference
     * @param arg3 The window input operator
//     * @throws StandardException
     */
    public void init(Object arg1, Object arg2, Object arg3) {  // throws StandardException {

        // Ranking window functions get their operand columns from from the ORDER BY clause.<br/>
        // Here, we inspect and validate the window ORDER BY clause (within OVER clause) to find
        // the columns with which to create the ranking function node.
        List<OrderedColumn> orderByList = ((WindowDefinitionNode)arg2).getOrderByList();
        if (orderByList == null || orderByList.isEmpty()) {
            SanityManager.THROWASSERT("Missing required ORDER BY clause for LAST_VALUE window function.");
        }
        if (arg3 == null) {
            SanityManager.THROWASSERT("Missing required input operand for LAST_VALUE window function.");
        }

        try {
            super.init(arg3, arg1, Boolean.FALSE, "LAST_VALUE", arg2);

            // TODO: JC - return type is same as input type
            setType(this.operand.getTypeServices());
        } catch (StandardException e) {
           throw new RuntimeException("LAST_VALUE init: "+ e.getLocalizedMessage());
        }
    }

    @Override
    public String getName() {
        return getAggregateName();
    }

    @Override
    public boolean isScalarAggregate() {
        return false;
    }

    @Override
    public ValueNode[] getOperands() {
        return new ValueNode[]{operand};
    }

    @Override
    protected void setOperands(ValueNode[] operands) throws StandardException {
        if (operands != null && operands.length > 0) {
            this.operand = operands[0];
        }
    }

// TODO: JC - commented code below will be needed when we support aggregate functions as arguments to LAST_VALUE(x)
//    @Override
//    public ValueNode replaceAggregatesWithColumnReferences(ResultColumnList rcl,
//                                                           int tableNumber) throws StandardException {
//        return aggregateFunction.replaceAggregatesWithColumnReferences(rcl, tableNumber);
//    }
//
//    @Override
//    public ValueNode bindExpression(FromList fromList,
//                                    SubqueryList subqueryList,
//                                    List<AggregateNode> aggregateVector) throws StandardException {
//        // DB-2086 - Vector.remove() calls node1.isEquivalent(node2), not node1.equals(node2), which
//        // returns true for identical aggregate nodes removing the first aggregate, not necessarily
//        // this one. We need to create a tmp Vector and add all Agg nodes found but this delegate by
//        // checking for object identity (==)
//        List<AggregateNode> tmp = new ArrayList<AggregateNode>();
//        aggregateFunction.bindExpression(fromList,subqueryList,tmp);
//
//        // We don't want to be in this aggregateVector - we add all aggs found during bind except
//        // this delegate.
//        // We want to bind the wrapped aggregate (and operand, etc) but we don't want to show up
//        // in this list as an aggregate. The list will be handed to GroupByNode, which we don't
//        // want doing the work.  Window function code will handle the window function aggregates
//        for (AggregateNode aggFn : tmp) {
//            if (aggregateFunction != aggFn) {
//                aggregateVector.add(aggFn);
//            }
//        }
//
//        // Now that delegate is bound, set some required fields on this
//        // TODO: What all is required?
//        this.operator = aggregateFunction.operator;
//        return this;
//    }

}
