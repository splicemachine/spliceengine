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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.conn.SessionProperties;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by yxia on 5/17/18.
 */
public class SetSessionPropertyNode extends MiscellaneousStatementNode {
    Properties sessionProperties = new Properties();

    /**
     * Initializer for a SetSessionPropertyNode
     *
     * @param properties	list of session properties

     *
     */
    public void init(Object properties) throws StandardException
    {
        List<Pair<String, String>> propertyList = (ArrayList)properties;
        for (Pair<String, String> pair: propertyList) {
            Pair<SessionProperties.PROPERTYNAME, String> validatedPair = SessionProperties.validatePropertyAndValue(pair);
            sessionProperties.put(validatedPair.getFirst(), validatedPair.getSecond());
        }
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
            return super.toString() + "sessionProperties: " + (sessionProperties == null? "null" : sessionProperties.toString()) ;
        }
        else
        {
            return "";
        }
    }

    public String statementToString()
    {
        return "SET SESSION PROPERTY";
    }

    /**
     * Create the Constant information that will drive the guts of Execution.
     *
     * @exception StandardException        Thrown on failure
     */
    public ConstantAction makeConstantAction() throws StandardException
    {
        return	getGenericConstantActionFactory().getSetSessionPropertyConstantAction(sessionProperties);
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
        // The generated java is the expression:
        // return ResultSetFactory.getMiscResultSet(this )

        acb.pushGetResultSetFactoryExpression(mb);

        acb.pushThisAsActivation(mb); // first arg

        mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getMiscResultSet",
                ClassName.ResultSet, 1);
    }

    /**
     * Returns the type of activation this class
     * generates.
     *
     * @return  NEED_PARAM_ACTIVATION or
     *			NEED_NOTHING_ACTIVATION depending on params
     *
     */
    int activationKind()
    {
        return StatementNode.NEED_NOTHING_ACTIVATION;
    }
}
