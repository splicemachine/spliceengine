/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.types.*;

import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * This node represents the SECOND function, which returns the "second" part of a time / timestamp
 * This node provides two compatibility options:
 * - SPLICE: SECOND takes one time / timestamp (or time/timestamp like string) operand and returns a double
 *     example: SECOND('10:12:45.123') -> 45.123
 * - DB2: SECOND takes one time / timestamp (or time / timestamp like string) operand and an optional integer constant
 *   if only the first operand is provided, SECOND returns an integer: SECOND('10:12:45.123') -> 45
 *   if the additional scale (s) operand is provided, SECOND returns a DECIMAL(2+s, s): SECOND('10:12:45.123') -> 45.123
 */
public class SecondFunctionNode extends OperatorNode {
    private String compatibility;

    public SecondFunctionNode(ValueNode op1, ValueNode op2, ContextManager cm) {
        setContextManager(cm);
        setNodeType(C_NodeTypes.SECOND_FUNCTION_NODE);
        operands = new ArrayList<>();
        operands.add(op1);
        if (op2 != null) {
            operands.add(op2);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException {
        bindOperands(fromList, subqueryList, aggregateVector);

        if (operands.get(0).requiresTypeFromContext() || operands.get(0).getTypeId().isStringTypeId()) {
            castOperandAndBindCast(0, DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.TIME));
        }

        int operandType = operands.get(0).getTypeId().getJDBCTypeId();
        if (operandType != Types.TIME && operandType != Types.TIMESTAMP) {
            throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE,
                    "SECONDS", operands.get(0).getTypeId().getSQLTypeName());
        }

        compatibility = getCompilerContext().getSecondFunctionCompatibilityMode().toLowerCase();
        switch (compatibility) {
            case "db2":
                return bindDb2();
            case "splice":
            default:
                return bindSplice();
        }
    }

    private ValueNode bindSplice() throws StandardException {
        if (operands.size() != 1) {
            throw StandardException.newException(SQLState.LANG_SYNTAX_ERROR,
                    "SECOND() expects 1 operand in SPLICE compatibility mode");
        }
        methodName = "getSecondsAndFractionOfSecondAsDouble";
        setType(new DataTypeDescriptor(TypeId.DOUBLE_ID, operands.get(0).getTypeServices().isNullable() ));
        return this;
    }

    private ValueNode bindDb2() throws StandardException {
        boolean isNullable = operands.get(0).getTypeServices().isNullable();
        switch(operands.size()) {
            case 1:
                methodName = "getSecondsAsInt";
                setType(new DataTypeDescriptor(TypeId.INTEGER_ID, isNullable));
                break;
            case 2:
                methodName = "getSecondsAndFractionOfSecondAsDecimal";
                TypeId decimalTypeId = TypeId.getBuiltInTypeId(Types.DECIMAL);
                assert decimalTypeId != null;
                if (operands.get(1).requiresTypeFromContext() ||
                    !operands.get(1).isConstantExpression() ||
                    !operands.get(1).getTypeId().isIntegerNumericTypeId()) {
                    throw StandardException.newException(SQLState.LANG_INVALID_FUNCTION_ARG_TYPE,
                            operands.get(1).requiresTypeFromContext() ? "?" : operands.get(1).getTypeId().getSQLTypeName(),
                            2,
                            "SECOND");
                }
                int scale = (Integer)operands.get(1).getConstantValueAsObject();
                if (scale < 1 || scale > 12) {
                    throw StandardException.newException(SQLState.LANG_INVALID_FUNCTION_ARGUMENT,
                            scale,
                            "SECOND");
                }
                int precision = scale + 2;
                setType(new DataTypeDescriptor(
                        decimalTypeId, precision, scale,
                        isNullable, DataTypeUtilities.computeMaxWidth(precision, scale)));
                break;
            default:
                throw StandardException.newException(SQLState.LANG_SYNTAX_ERROR,
                        "SECOND() expects 1 or 2 operands in DB2 compatibility mode");

        }
        return this;
    }

    @Override
    public void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb) throws StandardException
    {
        String resultTypeName = getTypeCompiler().interfaceName();
        operands.get(0).generateExpression(acb, mb);

        acb.generateNull(mb, getTypeCompiler(), getTypeServices());
        mb.cast(resultTypeName);
        mb.callMethod(VMOpcode.INVOKEINTERFACE, null, methodName, resultTypeName, 1);
    }
}
