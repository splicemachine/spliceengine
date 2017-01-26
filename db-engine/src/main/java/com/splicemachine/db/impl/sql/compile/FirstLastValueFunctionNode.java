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

import java.util.Collections;
import java.util.List;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.shared.common.reference.SQLState;
import org.spark_project.guava.collect.Lists;

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

        // first/last_value() is only deterministic when the window definition has an ORDER BY clause, that is, we want
        // column values relative to the current row in some ordered set.  However, we're not making the restriction here
        // to require an ORDER BY clause.  See DB-3976
        if (arg3 == null) {
            throw StandardException.newException(SQLState.LANG_MISSING_FIRST_LAST_VALUE_ARG);
        }
        super.init(arg3, arg1, Boolean.FALSE, arg5, arg2, arg4);
        setType(this.operand.getTypeServices());
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
    public FormatableHashtable getFunctionSpecificArgs() {
        FormatableHashtable container = new FormatableHashtable();
        container.put(FirstLastValueFunctionDefinition.IGNORE_NULLS, this.isIgnoreNulls());
        return container;
    }
}
