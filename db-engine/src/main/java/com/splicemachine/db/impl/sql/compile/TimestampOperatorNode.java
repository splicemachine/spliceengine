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

import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.impl.sql.catalog.Aggregate;

import java.sql.Types;

import java.util.List;
import java.util.Vector;

/**
 * The TimestampOperatorNode class implements the timestamp( date, time) function.
 */

public class TimestampOperatorNode extends BinaryOperatorNode
{

    /**
     * Initailizer for a TimestampOperatorNode.
     *
     * @param date The date
     * @param time The time
     */

    public void init( Object date,
                      Object time)
    {
        leftOperand = (ValueNode) date;
        rightOperand = (ValueNode) time;
        operator = "timestamp";
        methodName = "getTimestamp";
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
    @Override
	public ValueNode bindExpression( FromList fromList, SubqueryList subqueryList, List<AggregateNode> aggregateVector)  throws StandardException {
		leftOperand = leftOperand.bindExpression(fromList, subqueryList, 
			aggregateVector);
		rightOperand = rightOperand.bindExpression(fromList, subqueryList, 
			aggregateVector);

		//Set the type if there is a parameter involved here 
		if (leftOperand.requiresTypeFromContext()) {
			leftOperand.setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor( Types.DATE));
		}
		//Set the type if there is a parameter involved here 
		if (rightOperand.requiresTypeFromContext()) {
			rightOperand.setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor( Types.TIME));
		}

		TypeId leftTypeId = leftOperand.getTypeId();
        TypeId rightTypeId = rightOperand.getTypeId();
        if( !(leftOperand.requiresTypeFromContext() || leftTypeId.isStringTypeId() || leftTypeId.getJDBCTypeId() == Types.DATE))
            throw StandardException.newException(SQLState.LANG_BINARY_OPERATOR_NOT_SUPPORTED, 
                                                 operator, leftTypeId.getSQLTypeName(), rightTypeId.getSQLTypeName());
        if( !(rightOperand.requiresTypeFromContext() || rightTypeId.isStringTypeId() || rightTypeId.getJDBCTypeId() == Types.TIME))
            throw StandardException.newException(SQLState.LANG_BINARY_OPERATOR_NOT_SUPPORTED, 
                                                 operator, leftTypeId.getSQLTypeName(), rightTypeId.getSQLTypeName());
        setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor( Types.TIMESTAMP));
		return genSQLJavaSQLTree();
	} // end of bindExpression

    /**
	 * Do code generation for this binary operator.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the code to place the code
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
		throws StandardException
	{
        acb.pushDataValueFactory(mb);
		leftOperand.generateExpression(acb, mb);
        mb.cast( ClassName.DataValueDescriptor);
		rightOperand.generateExpression(acb, mb);
        mb.cast( ClassName.DataValueDescriptor);
        mb.callMethod( VMOpcode.INVOKEINTERFACE, null, methodName, ClassName.DateTimeDataValue, 2);
    } // end of generateExpression

    /**
     * Return whether or not this expression tree is cloneable.
     *
     * @return boolean	Whether or not this expression tree is cloneable.
     */
    public boolean isCloneable()
    {
        return leftOperand.isCloneable() && rightOperand.isCloneable();
    }


    /**
     * Return a clone of this node.
     *
     * @return ValueNode	A clone of this node.
     *
     * @exception StandardException			Thrown on error
     */
    public ValueNode getClone() throws StandardException {
        TimestampOperatorNode newTS = (TimestampOperatorNode) getNodeFactory().getNode(
            C_NodeTypes.TIMESTAMP_OPERATOR_NODE,
            leftOperand,
            rightOperand,
            getContextManager());

        newTS.copyFields(this);
        return newTS;
    }

}
