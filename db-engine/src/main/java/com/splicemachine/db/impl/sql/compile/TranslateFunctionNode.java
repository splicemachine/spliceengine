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
