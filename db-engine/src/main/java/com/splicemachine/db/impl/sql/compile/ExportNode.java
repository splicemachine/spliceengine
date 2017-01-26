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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;

import java.util.List;

/**
 * Export Node
 * <p/>
 * EXAMPLE:
 * <p/>
 * EXPORT('/dir', true, 3, 'utf-8', ',', '"') select a, b, sqrt(c) from table1 where a > 100;
 */
public class ExportNode extends DMLStatementNode {

    private static final int EXPECTED_ARGUMENT_COUNT = 6;
    public static final int DEFAULT_INT_VALUE = Integer.MIN_VALUE;

    private StatementNode node;
    /* HDFS, local, etc */
    private String exportPath;
    private boolean compression;
    private int replicationCount;
    private String encoding;
    private String fieldSeparator;
    private String quoteCharacter;

    @Override
    int activationKind() {
        return StatementNode.NEED_NOTHING_ACTIVATION;
    }

    @Override
    public String statementToString() {
        return "Export";
    }

    @Override
    public void init(Object statementNode, Object argumentsVector) throws StandardException {
        if (!(argumentsVector instanceof List) || ((List) argumentsVector).size() != EXPECTED_ARGUMENT_COUNT) {
            throw StandardException.newException(SQLState.LANG_DB2_NUMBER_OF_ARGS_INVALID, "EXPORT");
        }
        List argsList = (List) argumentsVector;
        this.node = (StatementNode) statementNode;

        this.exportPath = stringValue(argsList.get(0));
        this.compression = booleanValue(argsList.get(1));
        this.replicationCount = intValue(argsList.get(2));
        this.encoding = stringValue(argsList.get(3));
        this.fieldSeparator = stringValue(argsList.get(4));
        this.quoteCharacter = stringValue(argsList.get(5));
    }

    @Override
    public void optimizeStatement() throws StandardException {
        node.optimizeStatement();
    }

    @Override
    public void bindStatement() throws StandardException {
        node.bindStatement();
    }

    @Override
    public void generate(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException {
        acb.pushGetResultSetFactoryExpression(mb);
        // parameter
        node.generate(acb, mb);
        acb.pushThisAsActivation(mb);
        int resultSetNumber = getCompilerContext().getNextResultSetNumber();
        mb.push(resultSetNumber);
        mb.push(exportPath);
        mb.push(compression);
        mb.push(replicationCount);
        mb.push(encoding);
        mb.push(fieldSeparator);
        mb.push(quoteCharacter);

        /* Save result description of source node for use in export formatting. */
        mb.push(acb.addItem(node.makeResultDescription()));

        mb.callMethod(VMOpcode.INVOKEINTERFACE, null, "getExportResultSet", ClassName.NoPutResultSet, 10);
    }

    @Override
    public ResultDescription makeResultDescription() {
        DataTypeDescriptor dtd1 = new DataTypeDescriptor(TypeId.getBuiltInTypeId(TypeId.LONGINT_NAME), true);
        DataTypeDescriptor dtd2 = new DataTypeDescriptor(TypeId.getBuiltInTypeId(TypeId.LONGINT_NAME), true);
        ResultColumnDescriptor[] columnDescriptors = new GenericColumnDescriptor[2];
        columnDescriptors[0] = new GenericColumnDescriptor("Row Count", dtd1);
        columnDescriptors[1] = new GenericColumnDescriptor("Total Time (ms)", dtd2);
        String statementType = statementToString();
        return getExecutionFactory().getResultDescription(columnDescriptors, statementType);
    }

    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        if (node != null) {
            node = (StatementNode) node.accept(v, this);
        }
    }

    private static boolean isNullConstant(Object object) {
        return object instanceof ConstantNode && ((ConstantNode) object).isNull();
    }



    private static String stringValue(Object object) throws StandardException {
        // MethodBuilder can't handle null, so we use empty string when the user types NULL as argument
        if (isNullConstant(object)) {
            return "";
        }

        if (object instanceof CharConstantNode) {
           return ((CharConstantNode) object).getString();
        }

        throw newException(object);
    }

    private static int intValue(Object object) throws StandardException {
        if (isNullConstant(object)) {
            return DEFAULT_INT_VALUE;
        }

        if (object instanceof NumericConstantNode) {
            return ((NumericConstantNode) object).getValue().getInt();
        }

        throw newException(object);
    }

    private static boolean booleanValue(Object object) throws StandardException {
        if (isNullConstant(object)) {
            return true;
        }

        if (object instanceof BooleanConstantNode) {
            return ((BooleanConstantNode) object).isBooleanTrue();
        }

        throw newException(object);
    }


    private static StandardException newException(Object object) {
        if (object instanceof ConstantNode) {
            ConstantNode constantNode = (ConstantNode) object;
            return StandardException.newException(SQLState.EXPORT_PARAMETER_VALUE_IS_WRONG, constantNode.getValue().toString());
        }

        return StandardException.newException(SQLState.EXPORT_PARAMETER_VALUE_IS_WRONG, object.toString());
    }

}