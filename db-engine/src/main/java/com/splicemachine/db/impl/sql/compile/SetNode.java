/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
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
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.RowOrdering;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.conn.SessionProperties;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.utils.Pair;

import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import static com.splicemachine.db.iapi.types.TypeId.VARCHAR_NAME;

/**
 * Created by msirek on Nov. 21, 2019.
 */
public class SetNode extends MiscellaneousStatementNode {
    protected ValueNodeList assignedColumnsList;
    protected ValueNodeList newValuesList;

    /**
     * Initializer for a SignalNode
     *
     * @param columnList The specification of column values to alter.
     */
    public void init(Object columnList, Object newValuesList) throws StandardException {
        assignedColumnsList = (ValueNodeList) columnList;
        this.newValuesList = (ValueNodeList) newValuesList;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */
    public String toString() {
        if (SanityManager.DEBUG) {
            return super.toString() + "Set: " + commonString();
        } else {
            return "";
        }
    }

    private String commonString() {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < assignedColumnsList.size(); i++) {
            sb.append(String.format("%s = %s", assignedColumnsList.elementAt(i), newValuesList.elementAt(i)));
            if (i != assignedColumnsList.size() - 1)
                sb.append(", ");
        }
        return sb.toString();
    }

    public String statementToString() {
        return "SET " + commonString();
    }

    public void bindStatement() throws StandardException {
        // We just need select privilege on the expressions
        getCompilerContext().pushCurrentPrivType(Authorizer.SELECT_PRIV);

        FromList fromList = (FromList) getNodeFactory().getNode(
        C_NodeTypes.FROM_LIST,
        getNodeFactory().doJoinOrderOptimization(),
        getContextManager());

        assignedColumnsList.bindExpression(fromList, null, null);
        newValuesList.bindExpression(fromList, null, null);

        getCompilerContext().popCurrentPrivType();
    }

    /**
     * Accept the visitor for all visitable children of this node.
     *
     * @param v the visitor
     */
    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        if (assignedColumnsList != null) {
            assignedColumnsList.accept(v, this);
        }
        if (newValuesList != null) {
            newValuesList.accept(v, this);
        }
    }


    private LocalField generateListAsArray(ExpressionClassBuilder acb,
                                           MethodBuilder mb) throws StandardException {
        int listSize = newValuesList.size();
        LocalField arrayField = acb.newFieldDeclaration(
        Modifier.PRIVATE, ClassName.DataValueDescriptor + "[]");

        /* Assign the initializer to the DataValueDescriptor[] field */
        MethodBuilder cb = acb.getConstructor();
        cb.pushNewArray(ClassName.DataValueDescriptor, listSize);
        cb.setField(arrayField);


        /* Set the array elements that are constant */
        MethodBuilder nonConstantMethod = null;
        MethodBuilder currentConstMethod = cb;

        for (int index = 0; index < listSize; index++) {
            ValueNode dataLiteral = (ValueNode) newValuesList.elementAt(index);
            MethodBuilder setArrayMethod = null;
            int numConstants = 0;

            if (dataLiteral instanceof ConstantNode) {
                numConstants++;

                /*if too many statements are added  to a  method,
                 *size of method can hit  65k limit, which will
                 *lead to the class format errors at load time.
                 *To avoid this problem, when number of statements added
                 *to a method is > 2048, remaing statements are added to  a new function
                 *and called from the function which created the function.
                 *See Beetle 5135 or 4293 for further details on this type of problem.
                 */
                if (currentConstMethod.statementNumHitLimit(numConstants)) {
                    MethodBuilder genConstantMethod = acb.newGeneratedFun("void", Modifier.PRIVATE);
                    currentConstMethod.pushThis();
                    currentConstMethod.callMethod(VMOpcode.INVOKEVIRTUAL,
                    (String) null,
                    genConstantMethod.getName(),
                    "void", 0);
                    //if it is a generate function, close the metod.
                    if (currentConstMethod != cb) {
                        currentConstMethod.methodReturn();
                        currentConstMethod.complete();
                    }
                    currentConstMethod = genConstantMethod;
                }
                setArrayMethod = currentConstMethod;
            } else {
                if (nonConstantMethod == null)
                    nonConstantMethod = acb.newGeneratedFun("void", Modifier.PROTECTED);
                setArrayMethod = nonConstantMethod;

            }

            // Build the DVD to add to the DVD array, pushing it to the stack
            // cast as a DataValueDescriptor.
            dataLiteral.generateExpression(acb, setArrayMethod);
            setArrayMethod.upCast(ClassName.DataValueDescriptor);

            // Move the built DVD into the DVD array, using the proper
            // constant or non-constant method.
            InListOperatorNode.setDVDItemInArray(setArrayMethod, arrayField, index, 0, 1);

        }

        // if a generated function was created to reduce the size of the methods close the functions.
        if (currentConstMethod != cb) {
            currentConstMethod.methodReturn();
            currentConstMethod.complete();
        }

        if (nonConstantMethod != null) {
            nonConstantMethod.methodReturn();
            nonConstantMethod.complete();
            mb.pushThis();
            mb.callMethod(VMOpcode.INVOKEVIRTUAL, (String) null, nonConstantMethod.getName(), "void", 0);
        }

        return arrayField;
    }

    private void generateInListValues(ExpressionClassBuilder acb, MethodBuilder mb) throws StandardException {

        MethodBuilder getNewDVDsMethod = acb.newExprFun();
        LocalField newDVDsArray = generateListAsArray(acb, getNewDVDsMethod);

        getNewDVDsMethod.getField(newDVDsArray);
        getNewDVDsMethod.methodReturn();
        getNewDVDsMethod.complete();
        acb.pushMethodReference(mb, getNewDVDsMethod);

    }

    /**
     * Generate code, need to push parameters
     *
     * @param acb The ActivationClassBuilder for the class being built
     * @param mb  the method  for the execute() method to be built
     * @throws StandardException Thrown on error
     */

    public void generate(ActivationClassBuilder acb,
                         MethodBuilder mb)
    throws StandardException {

        acb.pushGetResultSetFactoryExpression(mb);

        acb.pushThisAsActivation(mb);
        StringBuilder sb = new StringBuilder();
        if (assignedColumnsList != null) {

            MethodBuilder userExprFun = null;
            for (int i = 0; i < assignedColumnsList.size(); i++) {
                userExprFun = acb.newUserExprFun();

                assignedColumnsList.elementAt(i).generate(acb, userExprFun);
                userExprFun.methodReturn();
                userExprFun.complete();
                sb.append(userExprFun.getName());
                if (i != assignedColumnsList.size() - 1)
                    sb.append(".");
            }
        } else
            throw new RuntimeException();
        mb.push(sb.toString());
        generateInListValues(acb, mb);

        mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getSetResultSet",
        ClassName.NoPutResultSet, 3);
    }

    /**
     * Returns the type of activation this class
     * generates.
     *
     * @return NEED_NOTHING_ACTIVATION
     */
    int activationKind() {
        return StatementNode.NEED_NOTHING_ACTIVATION;
    }

}