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

import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.StatementType;
import java.util.Vector;


/**
 * A SetRoleNode is the root of a QueryTree that represents a SET ROLE
 * statement.
 */

public class SetRoleNode extends MiscellaneousStatementNode
{
    private String      name;
    private int         type;

    /**
     * Initializer for a SetRoleNode
     *
     * @param roleName  The name of the new role, null if NONE specified
     * @param type      Type of role name could be USER or dynamic parameter
     *
     */
    public void init(Object roleName, Object type)
    {
        this.name = (String) roleName;
        if (type != null) {
            this.type = ((Integer)type).intValue();
        }
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return  This object as a String
     */

    public String toString()
    {
        if (SanityManager.DEBUG) {
            return super.toString() +
                (type == StatementType.SET_ROLE_DYNAMIC ?
                 "roleName: ?\n" :
                 "rolename: " + name + "\n");
        } else {
            return "";
        }
    }

    public String statementToString()
    {
        return "SET ROLE";
    }

    /**
     * Create the Constant information that will drive the guts of
     * Execution.
     *
     * @exception StandardException         Thrown on failure
     */
    public ConstantAction   makeConstantAction() throws StandardException
    {
        return getGenericConstantActionFactory().
			getSetRoleConstantAction(name, type);
    }
    /**
     * Override: Generate code, need to push parameters
     *
     * @param acb   The ActivationClassBuilder for the class being built
     * @param mb the method  for the execute() method to be built
     *
     * @exception StandardException         Thrown on error
     */

    public void generate(ActivationClassBuilder acb,
                         MethodBuilder mb)
            throws StandardException
    {
        //generate the parameters for the DYNAMIC SET ROLE
        if (type == StatementType.SET_ROLE_DYNAMIC) {
            generateParameterValueSet(acb);
        }
        // The generated java is the expression:
        // return ResultSetFactory.getMiscResultSet(this )

        acb.pushGetResultSetFactoryExpression(mb);

        acb.pushThisAsActivation(mb); // first arg

        mb.callMethod(VMOpcode.INVOKEINTERFACE, (String)null,
					  "getMiscResultSet", ClassName.ResultSet, 1);
    }
    /**
     * Generate the code to create the ParameterValueSet, if necessary,
     * when constructing the activation.  Also generate the code to call
     * a method that will throw an exception if we try to execute without
     * all the parameters being set.
     *
     * @param acb   The ActivationClassBuilder for the class we're building
     *
     * @exception StandardException         Thrown on error
     */

    private void generateParameterValueSet(ActivationClassBuilder acb)
        throws StandardException
    {
        Vector parameterList = getCompilerContext().getParameterList();
        // parameter list size should be 1
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(parameterList != null &&
								 parameterList.size() == 1);
        }
        ParameterNode.generateParameterValueSet (acb, 1, parameterList);
    }

    /**
     * Override: Returns the type of activation this class
     * generates.
     *
     * @return  NEED_PARAM_ACTIVATION or
     *          NEED_NOTHING_ACTIVATION depending on params
     *
     */
    int activationKind()
    {
        Vector parameterList = getCompilerContext().getParameterList();
        /*
        ** We need parameters only for those that have parameters.
        */
        if (type == StatementType.SET_ROLE_DYNAMIC) {
            return StatementNode.NEED_PARAM_ACTIVATION;
        } else {
            return StatementNode.NEED_NOTHING_ACTIVATION;
        }
    }


	/**
	 * Override to allow committing of reading SYSROLES,
	 * cf. SetRoleConstantAction's call to userCommit to retain idle
	 * state. If atomic, that commit will fail.
	 *
	 * @return false
	 */
	public boolean isAtomic() {
		return false;
	}


}
