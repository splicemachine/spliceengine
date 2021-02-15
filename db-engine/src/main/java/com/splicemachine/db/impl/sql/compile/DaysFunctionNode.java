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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This node represents the DAYS function which returns the number of days since January 1, 1 CE
 */
public class DaysFunctionNode extends UnaryOperatorNode {

    public DaysFunctionNode(ValueNode operand, ContextManager cm) {
        setContextManager(cm);
        setNodeType(C_NodeTypes.DAYS_FUNCTION_NODE);
        super.init(operand,"DAYS", "getDays");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException {
        bindOperand(fromList, subqueryList, aggregateVector);

        if (getOperand().getTypeId().isStringTypeId()) {
            castOperandAndBindCast(DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DATE));
        }

        int operandType = getOperand().getTypeId().getJDBCTypeId();

        if (operandType != Types.DATE && operandType != Types.TIMESTAMP) {
                throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE,
                        "DAYS",
                        getOperand().getTypeId().getSQLTypeName());
        }

        setType(new DataTypeDescriptor(
                        TypeId.BIGINT_ID,
                        getOperand().getTypeServices().isNullable()
                )
        );

        return this;
    }

    void bindParameter() throws StandardException {
        // for parameter, if we don't know the type, assume it is CHAR
        getOperand().setType(
                new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.CHAR), true));
    }
}
