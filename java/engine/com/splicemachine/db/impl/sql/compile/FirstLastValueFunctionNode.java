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
 * Class that represents a call to the FIRST_VALUE() and LAST_VALUE() window functions.
 */
public final class FirstLastValueFunctionNode extends WindowFunctionNode {

    /**
     * Initializer. QueryTreeNode override.
     *
     * @param arg1 The function's definition class
     * @param arg2 The window definition or reference
     * @param arg3 The window input operator
     * @param arg4 Ignore nulls
     * @param arg5 Function name ("LAST_VALUE" or "FIRST_VALUE")
     * @throws StandardException
     */
    public void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) throws StandardException {

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
        super.init(arg3, arg1, Boolean.FALSE, arg5, arg2, arg4);
        setType(this.operand.getTypeServices());
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
    public FormatableHashtable getFunctionSpecificArgs() {
        FormatableHashtable container = new FormatableHashtable();
        container.put(FirstLastValueFunctionDefinition.IGNORE_NULLS, this.isIgnoreNulls());
        return container;
    }
}
