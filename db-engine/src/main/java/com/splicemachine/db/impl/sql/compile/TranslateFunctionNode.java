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
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataTypeUtilities;
import com.splicemachine.db.iapi.types.TypeId;

import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * TRANSLATE NODE
 * usage:
 * TRANSLATE(string_expression, outputTranslationTable, inputTranslationTable [, pad])
 * This function returns a new string, of type TYPEOF(string_expression)
 * If any operand is null, we return null
 * This new string is constructed the following way:
 * for each character c in string_expression:
 *      if c is present in inputTranslationTable (position n), use outputTranslationTable[n]
 *      else use c
 *
 * For example:
 * translate('ABC', 'b', 'B') = 'AbC'
 * translate('ABC', 'ba', 'BA') = 'abC'
 *
 * If outputTranslationTable is longer than inputTranslationTable, we ignore the excess characters
 * If inputTranslationTable is longer than outputTranslationTable, we pad outputTranslationTable with pad (or with ' ' if pad isn't specified)
 *
 * string_expression must be a char / varchar
 * inputTranslationTable and outputTranslationTable must be char / varchar, or char / varchar for bit data. If 'for bit data', then we reinterpret it as varchar
 * Pad must be a char / varchar of size 1
 *
 * This implementation is inspired from the db2 implementation of TRANSLATE:
 * https://www.ibm.com/support/knowledgecenter/SSEPEK_10.0.0/sqlref/src/tpc/db2z_bif_translate.html
 *
 */
public class TranslateFunctionNode extends OperatorNode {
    public TranslateFunctionNode(ValueNode expression, ValueNode outputTranslationTable, ValueNode inputTranslationTable, ValueNode pad, ContextManager cm) {
        setContextManager(cm);
        setNodeType(C_NodeTypes.TRANSLATE_FUNCTION_NODE);
        operator = "TRANSLATE";
        methodName = "translate";
        operands = new ArrayList<>();
        operands.add(expression);
        operands.add(outputTranslationTable);
        operands.add(inputTranslationTable);
        operands.add(pad);
        interfaceTypes = new ArrayList<>(Arrays.asList(ClassName.ConcatableDataValue, ClassName.StringDataValue, ClassName.StringDataValue, ClassName.StringDataValue));
        resultInterfaceType = ClassName.ConcatableDataValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException {
        bindOperands(fromList, subqueryList, aggregateVector);

        for (int i = 0; i < 3; ++i) {
            if (operands.get(i).requiresTypeFromContext()) {
                castOperandAndBindCast(i, DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR));
            }
        }
        for (int i = 1; i < 3; ++i) {
            if (operands.get(i).getTypeId().isBitTypeId()) {
                castOperandAndBindCast(i, DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, operands.get(i).getTypeServices().getMaximumWidth()));
                ((CastNode) operands.get(i)).setForSbcsData(true);
            }
        }
        for (int i = 1; i < 3; ++i) {
            if (!operands.get(i).getTypeId().isStringTypeId()) {
                throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE,
                        "TRANSLATE", operands.get(i).getTypeId().getSQLTypeName());
            }
        }
        if (operands.get(3) != null) {
            if (operands.get(3).requiresTypeFromContext()) {
                operands.get(3).setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, 1));
            }
            if (!operands.get(3).getTypeId().isStringTypeId()) {
                throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE,
                        "TRANSLATE", operands.get(3).getTypeId().getSQLTypeName());
            }
            if (operands.get(3).getTypeServices().getMaximumWidth() != 1) {
                throw StandardException.newException(
                        SQLState.LANG_INVALID_TRANSLATE_PADDING, operands.get(3).getTypeId().getSQLTypeName());
            }
        }
        setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                Types.VARCHAR,
                operands.stream().anyMatch(op -> op != null && op.getTypeServices().isNullable()),
                operands.get(0).getTypeServices().getMaximumWidth()));
        return this;
    }
}
