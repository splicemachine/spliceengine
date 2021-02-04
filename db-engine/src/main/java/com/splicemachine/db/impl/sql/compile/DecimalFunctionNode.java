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
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.types.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.Types;
import java.util.List;

/**
 * This node represents the DECIMAL function which is basically a function that produces a decimal either from a numeric
 * expression or from a string expression.
 */
@SuppressFBWarnings(value="HE_INHERITS_EQUALS_USE_HASHCODE", justification="DB-9277")
public class DecimalFunctionNode extends UnaryOperatorNode {

    String functionName;
    int precision = -1;
    int scale = -1;
    String decimalChar;

    public DecimalFunctionNode(String functionName, ValueNode operand, Integer precision, Integer scale, String decimalChar, ContextManager cm) {
        setContextManager(cm);
        setNodeType(C_NodeTypes.DECIMAL_FUNCTION_NODE);
        this.functionName = functionName;
        this.operand = operand;
        this.precision = precision == null ? -1 : precision.intValue();
        this.scale = scale == null ? -1 : scale.intValue();
        this.decimalChar = decimalChar == null ? "" : decimalChar;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException {
        bindOperand(fromList, subqueryList, aggregateVector);

        switch (operand.getTypeId().getTypeFormatId()) {
            case StoredFormatIds.TINYINT_TYPE_ID: // fallthrough
            case StoredFormatIds.SQL_TINYINT_ID: // fallthrough
            case StoredFormatIds.SMALLINT_TYPE_ID: // fallthrough
            case StoredFormatIds.SQL_SMALLINT_ID:
                if (precision == -1) precision = 5;
                break;
            case StoredFormatIds.INT_TYPE_ID: // fallthrough:
            case StoredFormatIds.SQL_INTEGER_ID:
                if (precision == -1) precision = 11;
                break;
            case StoredFormatIds.LONGINT_TYPE_ID: // fallthrough:
            case StoredFormatIds.SQL_LONGINT_ID:
                if (precision == -1) precision = 19;
                break;
            case StoredFormatIds.DECIMAL_TYPE_ID: // fallthrough:
            case StoredFormatIds.SQL_DECIMAL_ID: // fallthrough
            case StoredFormatIds.REAL_TYPE_ID: // fallthrough
            case StoredFormatIds.SQL_REAL_ID: // fallthrough:
            case StoredFormatIds.DOUBLE_TYPE_ID: // fallthrough:
            case StoredFormatIds.SQL_DOUBLE_ID:
                if (precision == -1) precision = 15;
                break;
            case StoredFormatIds.SQL_DECFLOAT_ID: // fallthrough:
            case StoredFormatIds.DECFLOAT_TYPE_ID:
                if (precision == -1) precision = 31;
                break;
            case StoredFormatIds.SQL_VARCHAR_ID: // fallthrough:
            case StoredFormatIds.VARCHAR_TYPE_ID: // fallthrough
            case StoredFormatIds.SQL_CHAR_ID: // fallthrough:
            case StoredFormatIds.CHAR_TYPE_ID:
                if (precision == -1) precision = 15;
                break;
            case StoredFormatIds.SQL_DATE_ID: // fallthrough:
            case StoredFormatIds.DATE_TYPE_ID:
                if (precision == -1) precision = 8;
                else if (precision < 8) {
                    throw StandardException.newException(SQLState.LANG_INVALID_DECIMAL_CONVERSION, "expression", precision, scale == -1 ? 0 : scale);
                }
                break;
            case StoredFormatIds.SQL_TIME_ID: // fallthrough:
            case StoredFormatIds.TIME_TYPE_ID:
                if (precision == -1) precision = 6;
                else if (precision < 6) {
                    throw StandardException.newException(SQLState.LANG_INVALID_DECIMAL_CONVERSION, "expression", precision, scale == -1 ? 0 : scale);
                }
                break;
            case StoredFormatIds.SQL_TIMESTAMP_ID: // fallthrough:
            case StoredFormatIds.TIMESTAMP_TYPE_ID:
                if (precision == -1) {
                    precision = 14 + SQLTimestamp.MAX_FRACTION_DIGITS;
                    scale = SQLTimestamp.MAX_FRACTION_DIGITS;
                } else {
                    if (scale == -1) {
                        scale = 0;
                        if (precision < 14) {
                            throw StandardException.newException(SQLState.LANG_INVALID_DECIMAL_CONVERSION, "expression", precision, scale);
                        }
                    }
                }
                break;
            default:
                throw StandardException.newException(SQLState.LANG_INVALID_DECIMAL_TYPE, operand.getTypeId().getSQLTypeName());
        }
        if (scale == -1) {
            scale = 0;
        }
        if (decimalChar.equals(" ")) {
            decimalChar = ",";
        }

        TypeId decimalTypeId = TypeId.getBuiltInTypeId(Types.DECIMAL);
        assert decimalTypeId != null;

        setType(new DataTypeDescriptor(decimalTypeId, precision,
                                       scale, operand.getTypeServices().isNullable(), DataTypeUtilities.computeMaxWidth(precision, scale)));
        setOperator("get_decimal_db2");
        setMethodName("getDecimalDb2");
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb) throws StandardException {
        operand.generateExpression(acb, mb);
        mb.upCast(ClassName.DataValueDescriptor);
        mb.push(precision);
        mb.push(scale);
        mb.push(decimalChar);
        mb.callMethod(VMOpcode.INVOKESTATIC, ClassName.DecimalUtil,
                      "getDecimalDb2", ClassName.DataValueDescriptor, 4);
    }
}
