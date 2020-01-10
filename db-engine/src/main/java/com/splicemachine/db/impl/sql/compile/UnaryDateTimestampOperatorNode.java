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
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.types.DateTimeDataValue;

import java.sql.Types;
import java.util.List;

/**
 * This class implements the timestamp( x) and date(x) functions.
 * <p/>
 * These two functions implement a few special cases of string conversions beyond the normal string to
 * date/timestamp casts.
 */
public class UnaryDateTimestampOperatorNode extends UnaryOperatorNode{
    private static final String TIMESTAMP_METHOD_NAME="getTimestamp";
    private static final String DATE_METHOD_NAME="getDate";

    /**
     * @param operand    The operand of the function
     * @param targetType The type of the result. Timestamp or Date.
     * @throws StandardException Thrown on error
     */

    public void init(Object operand,Object targetType)
            throws StandardException{
        setType((DataTypeDescriptor)targetType);
        switch(getTypeServices().getJDBCTypeId()){
            case Types.DATE:
                super.init(operand,"date",DATE_METHOD_NAME);
                break;

            case Types.TIMESTAMP:
                super.init(operand,"timestamp",TIMESTAMP_METHOD_NAME);
                break;

            default:
                if(SanityManager.DEBUG)
                    SanityManager.NOTREACHED();
                super.init(operand);
        }
    }

    /**
     * Called by UnaryOperatorNode.bindExpression.
     * <p/>
     * If the operand is a constant then evaluate the function at compile time. Otherwise,
     * if the operand input type is the same as the output type then discard this node altogether.
     * If the function is "date" and the input is a timestamp then change this node to a cast.
     *
     * @param fromList        The FROM list for the query this
     *                        expression is in, for binding columns.
     * @param subqueryList    The subquery list being built as we find SubqueryNodes
     * @param aggregateVector The aggregate vector being built as we find AggregateNodes
     * @throws StandardException Thrown on error
     * @return The new top of the expression tree.
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException{
        boolean isIdentity=false; // Is this function the identity operator?
        boolean operandIsNumber=false;

        bindOperand(fromList,subqueryList,aggregateVector);
        DataTypeDescriptor operandType=operand.getTypeServices();
        switch(operandType.getJDBCTypeId()){
            case Types.BIGINT:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.DECIMAL:
            case Types.NUMERIC:
            case Types.DOUBLE:
            case Types.FLOAT:
                if(TIMESTAMP_METHOD_NAME.equals(methodName))
                    invalidOperandType();
                operandIsNumber=true;
                break;

            case Types.CHAR:
            case Types.VARCHAR:
                break;

            case Types.DATE:
                if(!TIMESTAMP_METHOD_NAME.equals(methodName))
                    isIdentity=true;
                break;

            case Types.NULL:
                break;

            case Types.TIMESTAMP:
                if(TIMESTAMP_METHOD_NAME.equals(methodName))
                    isIdentity=true;
                break;

            default:
                invalidOperandType();
        }

        if(operand instanceof ConstantNode){
            DataValueFactory dvf=getLanguageConnectionContext().getDataValueFactory();
            DataValueDescriptor sourceValue=((ConstantNode)operand).getValue();
            DataValueDescriptor destValue=null;
            if(sourceValue.isNull()){
                destValue=(TIMESTAMP_METHOD_NAME.equals(methodName))
                        ?dvf.getNullTimestamp((DateTimeDataValue)null)
                        :dvf.getNullDate((DateTimeDataValue)null);
            }else{
                destValue=(TIMESTAMP_METHOD_NAME.equals(methodName))
                        ?dvf.getTimestamp(sourceValue):dvf.getDate(sourceValue);
            }
            return (ValueNode)getNodeFactory().getNode(C_NodeTypes.USERTYPE_CONSTANT_NODE,
                    destValue,getContextManager());
        }

        if(isIdentity)
            return operand;
        return this;
    } // end of bindUnaryOperator

    private void invalidOperandType() throws StandardException{
        throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE,
                getOperatorString(),getOperand().getTypeServices().getSQLstring());
    }

    /**
     * Do code generation for this unary operator.
     *
     * @param acb The ExpressionClassBuilder for the class we're generating
     * @param mb  The method the expression will go into
     * @throws StandardException Thrown on error
     */

    public void generateExpression(ExpressionClassBuilder acb,
                                   MethodBuilder mb)
            throws StandardException{
        acb.pushDataValueFactory(mb);
        operand.generateExpression(acb,mb);
        mb.cast(ClassName.DataValueDescriptor);
        mb.callMethod(VMOpcode.INVOKEINTERFACE,(String)null,methodName,getTypeCompiler().interfaceName(),1);
    } // end of generateExpression
}
