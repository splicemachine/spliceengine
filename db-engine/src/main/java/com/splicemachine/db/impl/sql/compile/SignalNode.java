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
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
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
 * Created by msirek on Nov. 18, 2019.
 */
public class SignalNode extends MiscellaneousStatementNode {
    protected String sqlState;
    protected ValueNode errorText;

    /**
     * Initializer for a SignalNode
     *
     * @param sqlState	The error code to return.

     *
     */
    public void init(Object sqlState, Object errorText) throws StandardException
    {
        this.sqlState = (String)sqlState;
        this.errorText = (ValueNode)errorText;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return	This object as a String
     */
    public String toString()
    {
        if (SanityManager.DEBUG)
        {
            String errMsg = errorText != null ? OperatorToString.opToString(errorText) : null;
            String errorTextString =
                (errMsg != null && errMsg.length() != 0) ?
                      ", " + OperatorToString.opToString(errorText) : "";
            return super.toString() + "Signal: " + (sqlState == null? "null" : sqlState) + errorTextString ;
        }
        else
        {
            return "";
        }
    }

    public String statementToString()
    {
        return "SIGNAL " + sqlState;
    }

    public void bindStatement() throws StandardException {
        // We just need select privilege on the expressions
        getCompilerContext().pushCurrentPrivType(Authorizer.SELECT_PRIV);

        FromList fromList = (FromList) getNodeFactory().getNode(
        C_NodeTypes.FROM_LIST,
        getNodeFactory().doJoinOrderOptimization(),
        getContextManager());


        if (errorText != null)
            errorText = errorText.bindExpression(fromList, null,  null);
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
        if (errorText != null) {
            errorText.accept(v, this);
        }
    }

    /**
     * Generate code, need to push parameters
     *
     * @param acb	The ActivationClassBuilder for the class being built
     * @param mb the method  for the execute() method to be built
     *
     * @exception StandardException		Thrown on error
     */

    public void generate(ActivationClassBuilder acb,
                         MethodBuilder mb)
            throws StandardException
    {
        acb.pushGetResultSetFactoryExpression(mb);

        acb.pushThisAsActivation(mb);
        mb.push(this.sqlState);

        if (errorText == null){
            mb.pushNull(ClassName.GeneratedMethod);
        }
        else {
            // this sets up the method and the static field.
            // generates:
            // 	Object userExprFun { }
            MethodBuilder userExprFun=acb.newUserExprFun();

            errorText.generateExpression(acb,userExprFun);
            userExprFun.methodReturn();

            // we are done modifying userExprFun, complete it.
            userExprFun.complete();
            acb.pushMethodReference(mb, userExprFun);
        }

        mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getSignalResultSet",
                ClassName.NoPutResultSet, 3);
    }

    /**
     * Returns the type of activation this class
     * generates.
     *
     * @return  NEED_NOTHING_ACTIVATION
     *
     */
    int activationKind()
    {
        return StatementNode.NEED_NOTHING_ACTIVATION;
    }
}
