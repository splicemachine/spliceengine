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
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;

import java.sql.Types;
import java.util.List;

/**
 * This node represents a unary arithmetic operator
 */

public class UnaryArithmeticOperatorNode extends UnaryOperatorNode{
    private final static int UNARY_PLUS=0;
    private final static int UNARY_MINUS=1;
    private final static int SQRT=2;
    private final static int ABSOLUTE=3;
    private final static String[] UNARY_OPERATORS={"+","-","SQRT","ABS/ABSVAL"};
    private final static String[] UNARY_METHODS={"plus","minus","sqrt","absolute"};

    private int operatorType;

    /**
     * Initializer for a UnaryArithmeticOperatorNode
     *
     * @param operand The operand of the node
     */
    public void init(Object operand){
        switch(getNodeType()){
            case C_NodeTypes.UNARY_PLUS_OPERATOR_NODE:
                operatorType=UNARY_PLUS;
                break;
            case C_NodeTypes.UNARY_MINUS_OPERATOR_NODE:
                operatorType=UNARY_MINUS;
                break;
            case C_NodeTypes.SQRT_OPERATOR_NODE:
                operatorType=SQRT;
                break;
            case C_NodeTypes.ABSOLUTE_OPERATOR_NODE:
                operatorType=ABSOLUTE;
                break;
            default:
                if(SanityManager.DEBUG){
                    SanityManager.THROWASSERT("init for UnaryArithmeticOperator called with wrong nodeType = "+getNodeType());
                }
                break;
        }
        init(operand,UNARY_OPERATORS[this.operatorType],
                UNARY_METHODS[this.operatorType]);
    }

    /**
     * Unary + and - require their type to be set if
     * they wrap another node (e.g. a parameter) that
     * requires type from its context.
     *
     * @see ValueNode#requiresTypeFromContext
     */
    public boolean requiresTypeFromContext() {
        return (operatorType == UNARY_PLUS || operatorType == UNARY_MINUS) && operand.requiresTypeFromContext();
    }

    /**
     * A +? or a -? is considered a parameter.
     */
    public boolean isParameterNode() {
        return (operatorType == UNARY_PLUS || operatorType == UNARY_MINUS) && operand.isParameterNode();
    }

    /**
     * For SQRT and ABS the parameter becomes a DOUBLE.
     * For unary + and - no change is made to the
     * underlying node. Once this node's type is set
     * using setType, then the underlying node will have
     * its type set.
     *
     * @throws StandardException Thrown if ?  parameter doesn't
     *                           have a type bound to it yet.
     *                           ? parameter where it isn't allowed.
     */

    void bindParameter() throws StandardException{
        if(operatorType==SQRT || operatorType==ABSOLUTE){
            operand.setType(
                    new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.DOUBLE),true));
            return;
        }

        //Derby-582 add support for dynamic parameter for unary plus and minus
        if(operatorType==UNARY_MINUS || operatorType==UNARY_PLUS)
            return;

        // Not expected to get here since only the above types are supported
        // but the super-class method will throw an exception
        super.bindParameter();

    }

    /**
     * Bind this operator
     *
     * @param fromList        The query's FROM list
     * @param subqueryList    The subquery list being built as we find SubqueryNodes
     * @param aggregateVector The aggregate vector being built as we find AggregateNodes
     * @throws StandardException Thrown on error
     * @return The new top of the expression tree.
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException{
        //Return with no binding, if the type of unary minus/plus parameter is not set yet.
        if(operand.requiresTypeFromContext() && ((operatorType==UNARY_PLUS || operatorType==UNARY_MINUS))
                && operand.getTypeServices()==null)
            return this;

        bindOperand(fromList,subqueryList,
                aggregateVector);

        if(operatorType==SQRT || operatorType==ABSOLUTE){
            bindSQRTABS();
        }else if(operatorType==UNARY_PLUS || operatorType==UNARY_MINUS){
            checkOperandIsNumeric(operand.getTypeId());
        }
        /*
		** The result type of a +, -, SQRT, ABS is the same as its operand.
		*/
        super.setType(operand.getTypeServices());
        return this;
    }

    /**
     * Only called for Unary +/-.
     */
    private void checkOperandIsNumeric(TypeId operandType) throws StandardException{
        if(!operandType.isNumericTypeId()){
            throw StandardException.newException(
                    SQLState.LANG_UNARY_ARITHMETIC_BAD_TYPE,
                    (operatorType==UNARY_PLUS)?"+":"-",
                    operandType.getSQLTypeName());
        }

    }

    /**
     * Do code generation for this unary plus operator
     *
     * @param acb The ExpressionClassBuilder for the class we're generating
     * @param mb  The method the expression will go into
     * @throws StandardException Thrown on error
     */

    public void generateExpression(ExpressionClassBuilder acb,
                                   MethodBuilder mb)
            throws StandardException{
		/* Unary + doesn't do anything.  Just return the operand */
        if(operatorType==UNARY_PLUS)
            operand.generateExpression(acb,mb);
        else
            super.generateExpression(acb,mb);
    }

    /**
     * Bind SQRT or ABS
     *
     * @throws StandardException Thrown on error
     */
    private void bindSQRTABS()
            throws StandardException{
        TypeId operandType;
        int jdbcType;

		/*
		** Check the type of the operand 
		*/
        operandType=operand.getTypeId();

		/*
	 	 * If the operand is not a build-in type, generate a bound conversion
		 * tree to build-in types.
		 */
        if(operandType.userType()){
            operand=operand.genSQLJavaSQLTree();
        }
		/* DB2 doesn't cast string types to numeric types for numeric functions  */

        jdbcType=operandType.getJDBCTypeId();

		/* Both SQRT and ABS are only allowed on numeric types */
        if(!operandType.isNumericTypeId())
            throw StandardException.newException(
                    SQLState.LANG_UNARY_FUNCTION_BAD_TYPE,
                    getOperatorString(),operandType.getSQLTypeName());

		/* For SQRT, if operand is not a DOUBLE, convert it to DOUBLE */
        if(operatorType==SQRT && jdbcType!=Types.DOUBLE){
            operand=(ValueNode)getNodeFactory().getNode(
                    C_NodeTypes.CAST_NODE,
                    operand,
                    new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.DOUBLE),true),
                    getContextManager());
            ((CastNode)operand).bindCastNodeOnly();
        }
    }

    /**
     * We are overwriting this method here because for -?/+?, we now know
     * the type of these dynamic parameters and hence we can do the parameter
     * binding. The setType method will call the binding code after setting
     * the type of the parameter
     */
    public void setType(DataTypeDescriptor descriptor) throws StandardException{
        if(operand.requiresTypeFromContext() && operand.getTypeServices()==null){
            checkOperandIsNumeric(descriptor.getTypeId());
            operand.setType(descriptor);
        }
        super.setType(descriptor);
    }
}
