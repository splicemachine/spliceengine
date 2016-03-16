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

import java.util.Collections;
import java.util.List;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.shared.common.reference.SQLState;
import org.sparkproject.guava.collect.Lists;

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

        // Lead/Lag is only deterministic when the window definition has an ORDER BY clause, that is, we want column
        // values relative to the current row in some ordered set.  However, we're not making the restriction here
        // to require an ORDER BY clause.  See DB-3976
        if (arg3 == null) {
            throw StandardException.newException(SQLState.LANG_MISSING_LEAD_LAG_ARG);
        }
        super.init(arg3, arg1, Boolean.FALSE, arg4, arg2, false);
        setType(this.operand.getTypeServices());
        if (arg5 != null) {
            this.offset = (int) arg5;
            if (offset <= 0 || offset >= Integer.MAX_VALUE) {
                throw StandardException.newException(SQLState.LANG_INVALID_LEAD_LAG_OFFSET, Long.toString(offset));
            }
        }
        if (arg6 != null) {
            // TODO: JC test to make sure given default value is same type as operand. Has to be after bind time?
            this.defaultValue = (ValueNode) arg6;
            throw StandardException.newException(SQLState.LANG_MISSING_LEAD_LAG_DEFAULT);
        } else {
//            this.defaultValue = this.operand.getTypeServices().getNullabilityType(true);
        }
    }

    @Override
    public String getName() {
        return getAggregateName();
    }

    @Override
    public List<ValueNode> getOperands() {
        return (operand != null ? Lists.newArrayList(operand) : Collections.EMPTY_LIST);
    }

    @Override
    public void replaceOperand(ValueNode oldVal, ValueNode newVal) {
        this.operand = newVal;
    }

    @Override
    public ValueNode bindExpression(FromList fromList, SubqueryList subqueryList, List<AggregateNode>
        aggregateVector) throws StandardException {
        ValueNode bound = super.bindExpression(fromList, subqueryList, aggregateVector);
        if (defaultValue == null) {
            // TODO: jc if default value is null, get a null instance of the same type as the operand
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
