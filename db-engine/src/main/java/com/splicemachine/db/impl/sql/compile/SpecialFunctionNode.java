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
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.Collections;
import java.util.List;

/**
     SpecialFunctionNode handles system SQL functions.
     A function value is either obtained by a method
     call off the LanguageConnectionContext or Activation.
     LanguageConnectionContext functions are state related to the connection.
     Activation functions are those related to the statement execution.

     Each SQL function takes no arguments and returns a SQLvalue.
     <P>
     Functions supported:
     <UL>
     <LI> USER
     <LI> CURRENT_USER
     <LI> CURRENT_ROLE
     <LI> SESSION_USER
     <LI> GROUP_USER
     <LI> SYSTEM_USER
     <LI> CURRENT SCHEMA
     <LI> CURRENT ISOLATION
     <LI> IDENTITY_VAL_LOCAL
     <LI> CURRENT SESSION_PROPERTY
     <LI> CURRENT SERVER


     </UL>


    <P>

     This node is used rather than some use of MethodCallNode for
     runtime performance. MethodCallNode does not provide a fast access
     to the current language connection or activatation, since it is geared
     towards user defined routines.


*/
@SuppressFBWarnings(value="HE_INHERITS_EQUALS_USE_HASHCODE", justification="DB-9277")
public class SpecialFunctionNode extends ValueNode
{
    /**
        Name of SQL function
    */
    String sqlName;

    /**
        Java method name
    */
    private String methodName;

    /**
        Return type of Java method.
    */
    private String methodType;

    /**
    */
    //private boolean isActivationCall;

    /**
     * Binding this special function means setting the result DataTypeServices.
     * In this case, the result type is based on the operation requested.
     *
     * @param fromList            The FROM list for the statement.  This parameter
     *                            is not used in this case.
     * @param subqueryList        The subquery list being built as we find
     *                            SubqueryNodes. Not used in this case.
     * @param aggregateVector    The aggregate vector being built as we find
     *                            AggregateNodes. Not used in this case.
     *
     * @return    The new top of the expression tree.
     *
     * @exception StandardException        Thrown on error
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException {
        DataTypeDescriptor dtd;
        int nodeType = getNodeType();
        switch (nodeType) {
        case C_NodeTypes.USER_NODE:
        case C_NodeTypes.CURRENT_USER_NODE:
        case C_NodeTypes.SYSTEM_USER_NODE:
            switch (nodeType)
            {
                case C_NodeTypes.USER_NODE:sqlName = "USER"; break;
                case C_NodeTypes.CURRENT_USER_NODE: sqlName = "CURRENT_USER"; break;
                case C_NodeTypes.SYSTEM_USER_NODE: sqlName = "SYSTEM_USER"; break;
                default: assert false;
            }
            methodName = "getCurrentUserId";
            methodType = "java.lang.String";
            
            //SQL spec Section 6.4 Syntax Rule 4 says that the collation type
            //of these functions will be the collation of character set
            //SQL_IDENTIFIER. In Derby's case, that will mean, the collation of
            //these functions will be UCS_BASIC. The collation derivation will
            //be implicit.
            dtd = DataDictionary.TYPE_SYSTEM_IDENTIFIER;
            break;

        case C_NodeTypes.SESSION_USER_NODE:
            methodName = "getSessionUserId";
            methodType = "java.lang.String";
            sqlName = "SESSION_USER";
            dtd = DataDictionary.TYPE_SYSTEM_IDENTIFIER;
            break;

        case C_NodeTypes.GROUP_USER_NODE:
            methodName = "getCurrentGroupUserDelimited";
            methodType = "java.lang.String";
            sqlName = "GROUP_USER";
            dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.VARCHAR, true, 32672);
            break;
        case C_NodeTypes.CURRENT_SCHEMA_NODE:
            sqlName = "CURRENT SCHEMA";
            methodName = "getCurrentSchemaName";
            methodType = "java.lang.String";

            //This is a Derby specific function but its collation type will
            //be based on the same rules as for SESSION_USER/CURRENT_USER etc.
            //ie there collation type will be UCS_BASIC. The collation
            //derivation will be implicit.
            dtd = DataDictionary.TYPE_SYSTEM_IDENTIFIER;
            break;

        case C_NodeTypes.CURRENT_ROLE_NODE:
            sqlName = "CURRENT_ROLE";
            methodName = "getCurrentRoleIdDelimited";
            methodType = "java.lang.String";
            dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                // use maxlen to accommodate names for multiple active roles
                Types.VARCHAR, true, 32672);
            //SQL spec Section 6.4 Syntax Rule 4 says that the collation type
            //of these functions will be the collation of character set
            //SQL_IDENTIFIER. In Derby's case, that will mean, the collation of
            //these functions will be UCS_BASIC. The collation derivation will
            //be implicit. (set by default)
            break;

        case C_NodeTypes.CURRENT_SESSION_PROPERTY_NODE:
            sqlName = "CURRENT SESSION_PROPERTY";
            methodName = "getCurrentSessionPropertyDelimited";
            methodType = "java.lang.String";
            dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.VARCHAR, true, 32672);
            //SQL spec Section 6.4 Syntax Rule 4 says that the collation type
            //of these functions will be the collation of character set
            //SQL_IDENTIFIER. In Derby's case, that will mean, the collation of
            //these functions will be UCS_BASIC. The collation derivation will
            //be implicit. (set by default)
            break;
        case C_NodeTypes.IDENTITY_VAL_NODE:
            sqlName = "IDENTITY_VAL_LOCAL";
            methodName = "getIdentityValue";
            methodType = "java.lang.Long";
            dtd = DataTypeDescriptor.getSQLDataTypeDescriptor("java.math.BigDecimal", 31, 0, true, 31);
            break;

        case C_NodeTypes.CURRENT_ISOLATION_NODE:
            sqlName = "CURRENT ISOLATION";
            methodName = "getCurrentIsolationLevelStr";
            methodType = "java.lang.String";
            dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, 2);
            //This is a Derby specific function but it's collation type will
            //be based on the same rules as for SESSION_USER/CURRENT_USER etc.
            //ie there collation type will be UCS_BASIC. The collation
            //derivation will be implicit. (set by default).
            break;
        case C_NodeTypes.CURRENT_SERVER_NODE:
            sqlName = "CURRENT SERVER";
            methodName = "getDbname";
            methodType = "java.lang.String";
            dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, 16);
            break;
        default:
            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT("Invalid type for SpecialFunctionNode " + nodeType);
            }
            dtd = null;
            break;
        }

        checkReliability(sqlName, CompilerContext.USER_ILLEGAL );
        setType(dtd);

        return this;
    }

    /**
     * Return the variant type for the underlying expression.
       All supported special functions are QUERY_INVARIANT

     *
     * @return    The variant type for the underlying expression.
     */
    protected int getOrderableVariantType()
    {
        return Qualifier.QUERY_INVARIANT;
    }

    /**
        Generate an expression that returns a DataValueDescriptor and
        calls a method off the language connection or the activation.
     *
     * @param acb    The ExpressionClassBuilder for the class being built
     * @param mb    The method the code to place the code
     *
     *
     * @exception StandardException        Thrown on error
     */
    public void generateExpression(ExpressionClassBuilder acb,
                                            MethodBuilder mb)
                                    throws StandardException
    {
        mb.pushThis();
        mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.Activation, "getLanguageConnectionContext",
                                             ClassName.LanguageConnectionContext, 0);
        int argCount = 0;

        if (methodName.equals("getCurrentRoleIdDelimited") ||
            methodName.equals("getCurrentGroupUserDelimited") ||
            methodName.equals("getCurrentSchemaName") ||
            methodName.equals("getCurrentUserId")) {

            acb.pushThisAsActivation(mb);
            argCount++;
        }

        mb.callMethod(VMOpcode.INVOKEINTERFACE,
                      (String) null, methodName, methodType, argCount);

        String fieldType = getTypeCompiler().interfaceName();
        LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, fieldType);

        acb.generateDataValue(mb, getTypeCompiler(),
                getTypeServices().getCollationType(), field);
    }

    /*
        print the non-node subfields
     */
    public String toString() {
        if (SanityManager.DEBUG)
        {
            return "sqlName: " + sqlName + "\n" +
                super.toString();
        }
        else
        {
            return "";
        }
    }
        
    protected boolean isEquivalent(ValueNode o)
    {
        if (isSameNodeType(o))
        {
            SpecialFunctionNode other = (SpecialFunctionNode)o;
            return methodName.equals(other.methodName);
        }
        return false;
    }

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
    public long nonZeroCardinality(long numberOfRows) throws StandardException {
        return 1;
    }

    @Override
    public boolean isConstantOrParameterTreeNode() {
        return true;
    }
}
