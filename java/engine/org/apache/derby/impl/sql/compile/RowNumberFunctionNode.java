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

package org.apache.derby.impl.sql.compile;

import java.sql.Types;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.TypeId;

/**
 * Class that represents a call to the ROW_NUMBER() window function.
 */
public class RowNumberFunctionNode extends WindowFunctionNode {
    private ValueNode[] operands;
    /**
     * Initializer. QueryTreeNode override.
     *
     * @param arg1 operands
     * @param arg2 The function's definition class
     * @param arg3 The window definition or reference
     * @param arg4 null
     * @throws org.apache.derby.iapi.error.StandardException
     */
    public void init(Object arg1, Object arg2, Object arg3, Object arg4) throws StandardException {
        super.init(((ValueNode[])arg1)[0], arg2, Boolean.FALSE, "ROW_NUMBER", arg3);
        this.operands = (ValueNode[])arg1;
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
    public boolean isScalarAggregate() {
        return false;
    }

    @Override
    public ValueNode[] getOperands() {
        return operands;
    }
}
