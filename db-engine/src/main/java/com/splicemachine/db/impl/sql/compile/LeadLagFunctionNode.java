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
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.shared.common.reference.SQLState;
import splice.com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collections;
import java.util.List;

/**
 * Class that represents a call to the LEAD() and LAG() window functions.
 */
@SuppressFBWarnings(value="HE_INHERITS_EQUALS_USE_HASHCODE", justification="DB-9277")
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

        // lead/lag always works on the entire partition, so ignore the window frame input
        WindowFrameDefinition newWindowFrame = new WindowFrameDefinition(true);
        newWindowFrame.setFrameMode(WindowFrameDefinition.FrameMode.ROWS);
        OverClause overClause = this.getWindow().getOverClause();
        if (!overClause.getFrameDefinition().isEquivalent(newWindowFrame)) {
            OverClause newOverClause = new OverClause.Builder(getContextManager())
                    .setPartition(overClause.getPartition())
                    .setOrderByClause(overClause.getOrderByClause())
                    .setFrameDefinition(newWindowFrame)
                    .build();

            this.getWindow().setOverClause(newOverClause);
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
