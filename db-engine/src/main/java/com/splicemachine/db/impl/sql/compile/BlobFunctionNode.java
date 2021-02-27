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
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.types.*;

import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class BlobFunctionNode extends OperatorNode {
    public BlobFunctionNode(ValueNode op1, ValueNode op2, ContextManager cm) {
        setContextManager(cm);
        setNodeType(C_NodeTypes.BLOB_FUNCTION_NODE);
        operator = "BLOB";
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

        if (operands.get(0).getTypeId() != null && !operands.get(0).getTypeId().isBitTypeId() && !operands.get(0).getTypeId().isStringTypeId()) {
            throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE,
                    "BLOB", operands.get(0).getTypeId().getSQLTypeName());
        }
        int length = 0;
        boolean isNullable = operands.get(0).getTypeServices() == null || operands.get(0).getTypeServices().isNullable();
        switch(operands.size()) {
            case 1:
                length = operands.get(0).getTypeServices() == null ? TypeId.BLOB_MAXWIDTH : operands.get(0).getTypeServices().getMaximumWidth();
                break;
            case 2:
                if (operands.get(1).requiresTypeFromContext() ||
                        !operands.get(1).isConstantExpression() ||
                        !operands.get(1).getTypeId().isIntegerNumericTypeId()) {
                    throw StandardException.newException(SQLState.LANG_INVALID_FUNCTION_ARG_TYPE,
                            operands.get(1).requiresTypeFromContext() ? "?" : operands.get(1).getTypeId().getSQLTypeName(),
                            2,
                            "BLOB");
                }
                length = (Integer)operands.get(1).getConstantValueAsObject();
                if (length < 0) {
                    throw StandardException.newException(SQLState.LANG_INVALID_FUNCTION_ARGUMENT,
                            length,
                            "BLOB");
                }
                break;
            default:
                assert false;
        }
        setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BLOB, isNullable, length));

        if (operands.get(0).requiresTypeFromContext()) {
            operands.get(0).setType(getTypeServices());
        }

        return this;
    }

    public void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb) throws StandardException
    {
        operands.get(0).generateExpression(acb, mb);
        MethodBuilder acbConstructor = acb.getConstructor();
        String resultTypeName = getTypeCompiler().interfaceName();

        /* field = method call */
        /* Allocate an object for re-use to hold the result of the operator */
        LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, resultTypeName);

        /*
         ** Store the result of the method call in the field, so we can re-use
         ** the object.
         */
        acb.generateNull(acbConstructor, getTypeCompiler(getTypeId()),
                getTypeServices());
        acbConstructor.setField(field);

        acb.generateNull(mb, getTypeCompiler(getTypeId()),
                getTypeServices());
        mb.dup();
        mb.setField(field); // targetDVD reference for the setValue method call
        mb.swap();
        mb.upCast(ClassName.DataValueDescriptor);
        mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.DataValueDescriptor,
                "setValue", "void", 1);

        mb.getField(field);

        // to leave the DataValueDescriptor value on the stack, since setWidth is void
        mb.dup();

        mb.push(getTypeServices().getMaximumWidth());
        mb.push(getTypeServices().getScale());
        mb.push(false);
        mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.VariableSizeDataValue,
                "setWidth", "void", 3);
    }
}
