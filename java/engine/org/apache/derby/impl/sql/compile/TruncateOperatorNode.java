/*

   Derby - Class org.apache.derby.impl.sql.compile.TimestampOperatorNode

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.compile;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * The TruncateOperatorNode class implements the trunc[ate]( {date | time} [, string] | {decimal , integer} ) function.
 */

public class TruncateOperatorNode extends BinaryOperatorNode {

    private static final Map<Integer,Pair> TYPES_TO_METHOD_NAMES;
    private static final String TRUNC_DECIMAL = "truncDecimal";
    static {
        TYPES_TO_METHOD_NAMES = new HashMap<Integer, Pair>(4);
        TYPES_TO_METHOD_NAMES.put(Types.TIMESTAMP, new Pair(ClassName.DateTimeDataValue, "truncTimestamp"));
        TYPES_TO_METHOD_NAMES.put(Types.DATE, new Pair(ClassName.DateTimeDataValue, "truncDate"));
        TYPES_TO_METHOD_NAMES.put(Types.DECIMAL, new Pair(ClassName.NumberDataValue, TRUNC_DECIMAL));
        TYPES_TO_METHOD_NAMES.put(Types.INTEGER, new Pair(ClassName.NumberDataValue, TRUNC_DECIMAL));
    }

    private String methodClassname;

    /**
     * Initializer for a TruncateOperatorNode.
     *
     * @param truncOperand The truncOperand
     * @param truncValue The truncation value
     */
    public void init( Object truncOperand,
                      Object truncValue) throws StandardException {
        leftOperand = (ValueNode) truncOperand;
        rightOperand = (ValueNode) truncValue;
        operator = "truncate";

        // do some validation...
        TypeId leftTypeId = leftOperand.getTypeId();
        if (leftTypeId != null) {
            int jdbcId = leftTypeId.getJDBCTypeId();
            if (jdbcId == Types.TIMESTAMP || jdbcId == Types.DATE) {
                if (rightOperand.getTypeId().getJDBCTypeId() != Types.CHAR) {
                    throw StandardException.newException(SQLState.LANG_TRUNCATE_EXPECTED_RIGHTSIDE_CHAR_TYPE, rightOperand.toString());
                }
            } else if (jdbcId == Types.DECIMAL || jdbcId == Types.INTEGER) {
                TypeId rightTypeId = rightOperand.getTypeId();
                if (rightTypeId == null || rightTypeId.getJDBCTypeId() != Types.INTEGER) {
                    throw StandardException.newException(SQLState.LANG_TRUNCATE_EXPECTED_RIGHTSIDE_INTEGER_TYPE, rightOperand.toString());
                }
            } else {
                throw StandardException.newException(SQLState.LANG_TRUNCATE_UNKNOWN_TYPE_OPERAND, leftOperand.toString());
            }
        } else if (! (truncOperand instanceof ColumnReference)) {
            // A ColumnReference will not have a type until bind time.
            // We put off further ColumnReference validation until then.
            // If we don't get a ColumnReference at this point, we can't handle the operand.
            throw StandardException.newException(SQLState.LANG_TRUNCATE_UNKNOWN_TYPE_OPERAND, leftOperand.toString());
        }
    }

    /**
	 * Bind this expression.  This means binding the sub-expressions,
	 * as well as figuring out what the return type is for this expression.
	 *
	 * @param fromList		The FROM list for the query this
	 *				expression is in, for binding columns.
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
	 * @param aggregateVector	The aggregate vector being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ValueNode bindExpression(
		FromList fromList, SubqueryList subqueryList,
		Vector	aggregateVector) throws StandardException {

		leftOperand = leftOperand.bindExpression(fromList, subqueryList, aggregateVector);
		rightOperand = rightOperand.bindExpression(fromList, subqueryList, aggregateVector);

        int operandType = leftOperand.getTypeId().getJDBCTypeId();
        Pair typeMethod = TYPES_TO_METHOD_NAMES.get(operandType);

        if (typeMethod == null) {
            throw StandardException.newException(SQLState.LANG_TRUNCATE_UNKNOWN_TYPE_OPERAND, leftOperand.toString());
        }
        this.methodName = typeMethod.methodName;
        this.methodClassname = typeMethod.className;

        // Default right side to integer (zero) when trunc decimal column ref
        // This is the first place we can verify this because a column ref's type is not known until bind time
        if (this.methodName.equals(TRUNC_DECIMAL) && rightOperand.getTypeId().getJDBCTypeId() != Types.INTEGER) {
            rightOperand = (ValueNode) getCompilerContext().getNodeFactory().getNode(C_NodeTypes.INT_CONSTANT_NODE, 0, getContextManager());
        }

		//Set the type if there is a parameter involved here
		if (leftOperand.requiresTypeFromContext()) {
			leftOperand.setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor(operandType));
		}
		//Set the type if there is a parameter involved here
		if (rightOperand != null && rightOperand.requiresTypeFromContext()) {
			rightOperand.setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor(rightOperand.getTypeId().getJDBCTypeId()));
		}

        DataTypeDescriptor typeDescriptor = leftOperand.getTypeServices();
        if (typeDescriptor == null) {
            typeDescriptor = DataTypeDescriptor.getBuiltInDataTypeDescriptor(operandType);
        }
        setType(typeDescriptor);
		return genSQLJavaSQLTree();
	} // end of bindExpression

    /**
	 * Do code generation for this binary operator.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the code to place the code
	 *
	 *
	 * @exception org.apache.derby.iapi.error.StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb) throws StandardException {
        acb.pushDataValueFactory(mb);
		leftOperand.generateExpression(acb, mb);
        mb.cast(ClassName.DataValueDescriptor);
		rightOperand.generateExpression(acb, mb);
        mb.cast(ClassName.DataValueDescriptor);
        mb.callMethod(VMOpcode.INVOKEINTERFACE, null, methodName, methodClassname, 2);
    } // end of generateExpression

    private static class Pair {
        final String methodName;
        final String className;

        public Pair(String className, String methodName) {
            this.className = className;
            this.methodName = methodName;
        }
    }
}
