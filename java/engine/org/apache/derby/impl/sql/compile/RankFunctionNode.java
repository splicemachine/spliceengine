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
import java.util.Vector;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.TypeId;

/**
 * Class that represents a call to the RANK() window function.
 */
public final class RankFunctionNode extends WindowFunctionNode {

    /**
     * Initializer. QueryTreeNode override.
     *
     * @param arg1 null (Operand)
     * @param arg2 The window definition or reference
     * @throws org.apache.derby.iapi.error.StandardException
     */
    public void init(Object arg1, Object arg2) throws StandardException {
        super.init(arg1, "RANK", arg2);
        setType(TypeId.getBuiltInTypeId(Types.BIGINT),
                TypeId.LONGINT_PRECISION,
                TypeId.LONGINT_SCALE,
                false,
                TypeId.LONGINT_MAXWIDTH);
    }

    /**
     * ValueNode override.
     *
     * @see org.apache.derby.impl.sql.compile.ValueNode#bindExpression
     */
    public ValueNode bindExpression(
        FromList fromList,
        SubqueryList subqueryList,
        Vector aggregateVector) throws StandardException {
        super.bindExpression(fromList, subqueryList, aggregateVector);
        return this;
    }

    @Override
    public ColumnReference getGeneratedRef() {
        // TODO: impl
        return null;
    }

    @Override
    public ResultColumn getGeneratedRC() {
        // TODO: impl
        return null;
    }

    @Override
    public AggregateNode getWrappedAggregate() {
        // TODO: impl
        return null;
    }
}
