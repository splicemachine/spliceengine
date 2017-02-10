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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.types.TypeId;

/**
 * Class that represents a call to the ROW_NUMBER() window function.
 */
public class RowNumberFunctionNode extends WindowFunctionNode {
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

        super.init(orderByList.get(0).getColumnExpression(), arg1, Boolean.FALSE, "ROW_NUMBER", arg2);

        setType( TypeId.getBuiltInTypeId( Types.BIGINT ),
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
    public List<ValueNode> getOperands() {
        List<OrderedColumn> orderedColumns = getWindow().getOrderByList();
        List<ValueNode> operands = new ArrayList<>(orderedColumns.size()+1);
        if (operand != null) {
            operands.add(operand);
        }
        for (OrderedColumn orderedColumn : orderedColumns) {
            operands.add(orderedColumn.getColumnExpression());
        }
        return operands;
    }

    @Override
    public void replaceOperand(ValueNode oldVal, ValueNode newVal) {
        for(OrderedColumn oc : getWindow().getOrderByList()) {
            if (oc.getColumnExpression() == oldVal) {
                oc.setColumnExpression(newVal);
            }
        }
    }

}
