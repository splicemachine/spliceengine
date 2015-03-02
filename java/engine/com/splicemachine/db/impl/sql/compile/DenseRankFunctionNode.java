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

import java.sql.Types;
import java.util.List;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.types.TypeId;

/**
 * Class that represents a call to the DENSE_RANK() window function.
 */
public final class DenseRankFunctionNode extends WindowFunctionNode {
    /**
     * Initializer. QueryTreeNode override.
     *
     * @param arg1 The function's definition class
     * @param arg2 The window definition or reference
     * @throws com.splicemachine.db.iapi.error.StandardException
     */
    public void init(Object arg1, Object arg2) throws StandardException {

        // Ranking window functions get their operand columns from from the ORDER BY clause.<br/>
        // Here, we inspect and validate the window ORDER BY clause (within OVER clause) to find
        // the columns with which to create the ranking function node.
        List<OrderedColumn> orderByList = ((WindowDefinitionNode)arg2).getOrderByList();
        if (orderByList == null || orderByList.isEmpty()) {
            SanityManager.THROWASSERT("Missing required ORDER BY clause for ranking window function.");
        }

        super.init(orderByList.get(0).getColumnExpression(), arg1, Boolean.FALSE, "DENSE_RANK", arg2);

        setType(TypeId.getBuiltInTypeId(Types.BIGINT),
                TypeId.LONGINT_PRECISION,
                TypeId.LONGINT_SCALE,
                false,
                TypeId.LONGINT_MAXWIDTH);
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
        List<OrderedColumn> orderedColumns = getWindow().getOrderByList();
        ValueNode[] operands = new ValueNode[orderedColumns.size()];
        int i =0;
        for (OrderedColumn orderedColumn : orderedColumns) {
            operands[i++] = orderedColumn.getColumnExpression();
        }
        return operands;
    }

    @Override
    protected void setOperands(ValueNode[] operands) throws StandardException {
        if (operands != null && operands.length > 0) {
            this.operand = operands[0];
            List<OrderedColumn> orderByList = getWindow().getOrderByList();
            if (orderByList != null && orderByList.size() > 0) {
                for (int i=0; i<operands.length; i++) {
                    orderByList.get(i).init(operands[i]);
                }
            }
        }
    }
}
