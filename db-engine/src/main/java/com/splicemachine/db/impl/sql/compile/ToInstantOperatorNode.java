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
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.Types;
import java.util.List;

@SuppressFBWarnings(value="HE_INHERITS_EQUALS_USE_HASHCODE", justification="DB-9277")
public final class ToInstantOperatorNode extends UnaryOperatorNode {

    /**
     * @inheritDoc
     * @throws StandardException if the operand type is string nor ref.
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException {

        bindOperand(fromList, subqueryList, aggregateVector);

        TypeId operandType = operand.getTypeId();
        switch (operandType.getJDBCTypeId())
        {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARBINARY:
            case Types.LONGVARCHAR:
            case Types.REF:
                break;

            default:
                throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE,
                        getOperatorString(),
                        operandType.getSQLTypeName());
        }

        // The return type is TIMESTAMP
        TypeId timestampTypeId = TypeId.getBuiltInTypeId(Types.TIMESTAMP);
        assert timestampTypeId != null;
        setType(new DataTypeDescriptor(timestampTypeId, operand.getTypeServices().isNullable()));
        setOperator("to_instant");
        setMethodName("ToInstant");
        return this;
    }

    @Override
    public void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb) throws StandardException {
        operand.generateExpression(acb, mb);
        mb.upCast(ClassName.DataValueDescriptor);
        mb.callMethod(VMOpcode.INVOKESTATIC, ClassName.RowIdUtil, "toInstant", ClassName.DateTimeDataValue, 1);
    }
}
