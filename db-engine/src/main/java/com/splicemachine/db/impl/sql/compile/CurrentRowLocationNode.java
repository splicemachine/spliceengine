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

import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.compiler.LocalField;

import java.lang.reflect.Modifier;

import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.error.StandardException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collections;
import java.util.List;

/**
 * The CurrentRowLocation operator is used by DELETE and UPDATE to get the
 * RowLocation of the current row for the target table.  The bind() operations
 * for DELETE and UPDATE add a column to the target list of the SelectNode
 * that represents the ResultSet to be deleted or updated.
 */

@SuppressFBWarnings(value="HE_INHERITS_EQUALS_USE_HASHCODE", justification="DB-9277")
public class CurrentRowLocationNode extends ValueNode
{
    /**
     * Binding this expression means setting the result DataTypeServices.
     * In this case, the result type is always the same.
     *
     * @param fromList            The FROM list for the statement.  This parameter
     *                            is not used in this case.
     * @param subqueryList        The subquery list being built as we find SubqueryNodes
     * @param aggregateVector    The aggregate vector being built as we find AggregateNodes
     *
     * @return    The new top of the expression tree.
     *
     * @exception StandardException        Thrown on error
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException {
        setType(new DataTypeDescriptor(TypeId.getBuiltInTypeId(TypeId.REF_NAME),
                        false        /* Not nullable */
                    )
                );
        return this;
    }

    /**
     * CurrentRowLocationNode is used in updates and deletes.  See generate() in
     * UpdateNode and DeleteNode to get the full overview of generate().  This
     * class is responsible for generating the method that will return the RowLocation
     * for the next row to be updated or deleted.
     *
     * This routine will generate a method of the form:
     *
     *        private SQLRef    fieldx;
     *
     *        ...
     *
     *        public DataValueDescriptor exprx()
     *                throws StandardException
     *        {
     *            return fieldx = <SQLRefConstructor>(
     *                                    "result set member".getRowLocation(),
     *                                    fieldx);
     *        }
     * and return the generated code:
     *    exprx()
     *
     * ("result set member" is a member of the generated class added by UpdateNode or
     * DeleteNode.)
     * This exprx function is used within another exprx function,
     * and so doesn't need a static field or to be public; but
     * at present, it has both.
     *
     * fieldx is a generated field that is initialized to null when the
     * activation is constructed.  getSQLRef will re-use fieldx on calls
     * after the first call, rather than allocate a new SQLRef for each call.
     *
     * @param acb    The ExpressionClassBuilder for the class being built
     *
     *
     * @exception StandardException        Thrown on error
     */
    public void generateExpression(ExpressionClassBuilder acb,
                                            MethodBuilder mbex)
                                    throws StandardException
    {
        /* Generate a new method */
        /* only used within the other exprFuns, so can be private */
        MethodBuilder mb = acb.newGeneratedFun(ClassName.DataValueDescriptor, Modifier.PROTECTED);

        /* Allocate an object for re-use to hold the result of the operator */
        LocalField field =
            acb.newFieldDeclaration(Modifier.PRIVATE, ClassName.RefDataValue);


        /* Fill in the body of the method
         * generates:
         *    return TypeFactory.getSQLRef(this.ROWLOCATIONSCANRESULTSET.getRowLocation());
         * and adds it to exprFun
         */

        mb.pushThis();
        mb.getField((String)null, acb.getRowLocationScanResultSetName(), ClassName.CursorResultSet);
        mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getRowLocation", ClassName.RowLocation, 0);


        acb.generateDataValue(mb, getTypeCompiler(),
                getTypeServices().getCollationType(), field);

        /*
        ** Store the result of the method call in the field, so we can re-use
        ** the object.
        */
//        mb.putField(field);

        /* Stuff the full expression into a return statement and add that to the
         * body of the new method.
         */
        mb.methodReturn();

        // complete the method
        mb.complete();

        /* Generate the call to the new method */
        mbex.pushThis();
        mbex.callMethod(VMOpcode.INVOKEVIRTUAL, (String) null, mb.getName(), ClassName.DataValueDescriptor, 0);
    }

    @Override
    protected boolean isEquivalent(ValueNode o)
    {
        return this == o;
    }

    @Override
    public List<? extends QueryTreeNode> getChildren() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public QueryTreeNode getChild(int index) {
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setChild(int index, QueryTreeNode newValue) {
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public CurrentRowLocationNode getClone() throws StandardException {
        CurrentRowLocationNode currentRowLocationNode = (CurrentRowLocationNode) getNodeFactory().getNode(
                C_NodeTypes.CURRENT_ROW_LOCATION_NODE,
                getContextManager());
        currentRowLocationNode.bindExpression(null, null, null);
        return currentRowLocationNode;
    }
}
