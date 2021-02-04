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
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataTypeUtilities;
import com.splicemachine.db.iapi.types.SQLTimestamp;
import com.splicemachine.db.iapi.types.TypeId;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.Types;
import java.util.Collections;
import java.util.List;

/**
 * This node represents the TYPEOF function which is basically a function that returns the type of an expression
 */
@SuppressFBWarnings(value="HE_INHERITS_EQUALS_USE_HASHCODE", justification="DB-9277")
public class TypeofOperatorNode extends UnaryOperatorNode {

    public TypeofOperatorNode(ValueNode operand, ContextManager cm)
    {
        setContextManager(cm);
        setNodeType(C_NodeTypes.TYPEOF_OPERATOR_NODE);
        this.operands = Collections.singletonList(operand);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException {
        bindOperand(fromList, subqueryList, aggregateVector);
        setType(TypeId.getBuiltInTypeId(Types.VARCHAR),
                false, 40);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb) throws StandardException {
        {
            mb.push(getOperand().getTypeServices().toString());

            acb.generateDataValue(mb, getTypeCompiler(),
                    getTypeServices().getCollationType(), (LocalField) null);
        }
    }
}
