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
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

/**
 * Class that represents a call to the LEAD() and LAG() window functions.
 */
public final class LeadLagFunctionNode extends WindowFunctionNode {

    private int offset = 1;
    // TODO: JC - handle default value
    private ValueNode defaultValue = null;

    /**
     * Initializer. QueryTreeNode override.
     *
     * @param arg1 The function's definition class
     * @param arg2 The window definition or reference
     * @param arg3 The window input operator
     * @param arg4 Function name ("LEAD" or "LAG")
     * @param arg5 offset
     * @param arg6 default value
     * @throws StandardException
     */
    public void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6) throws StandardException {

        // Ranking window functions get their operand columns from from the ORDER BY clause.<br/>
        // Here, we inspect and validate the window ORDER BY clause (within OVER clause) to find
        // the columns with which to create the ranking function node.
        List<OrderedColumn> orderByList = ((WindowDefinitionNode)arg2).getOrderByList();
        if (orderByList == null || orderByList.isEmpty()) {
            SanityManager.THROWASSERT("Missing required ORDER BY clause for LEAD, LAG window functions.");
        }
        if (arg3 == null) {
            SanityManager.THROWASSERT("Missing required input operand for LEAD, LAG window functionS.");
        }
        super.init(arg3, arg1, Boolean.FALSE, arg4, arg2, false);
        setType(this.operand.getTypeServices());
        if (arg5 != null) {
            this.offset = (int) arg5;
        }
        if (arg6 != null) {
            // TODO: test to make sure given default value is same type as operand. Has to be after bind time?
            this.defaultValue = (ValueNode) arg6;
        } else {
//            this.defaultValue = this.operand.getTypeServices().getNullabilityType(true);
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

    @Override
    public ValueNode bindExpression(FromList fromList, SubqueryList subqueryList, List<AggregateNode>
        aggregateVector) throws StandardException {
        ValueNode bound = super.bindExpression(fromList, subqueryList, aggregateVector);
        if (defaultValue == null) {
            // TODO: if default value is null, get a null instance of the same type as the operand
//            defaultValue = bound.getTypeServices().getNull();
        }
        return bound;
    }

    @Override
    public FormatableHashtable getFunctionSpecificArgs() {
        FormatableHashtable container = new FormatableHashtable();
        container.put(LeadLagFunctionDefinition.OFFSET, this.offset);
        container.put(LeadLagFunctionDefinition.DEFAULT_VALUE, this.defaultValue);
        return container;
    }
}
