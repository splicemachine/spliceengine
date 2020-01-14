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

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.reference.ClassName;

import java.sql.Types;

import java.util.List;

/**
 * This node represents a unary upper or lower operator
 *
 */

public class SimpleStringOperatorNode extends UnaryOperatorNode
{
	/**
	 * Initializer for a SimpleOperatorNode
	 *
	 * @param operand		The operand
	 * @param methodName	The method name
	 */

	public void init(Object operand, Object methodName)
	{
		super.init(operand, methodName, methodName);
	}

	/**
	 * Bind this operator
	 *
	 * @param fromList			The query's FROM list
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
	 * @param aggregateVector	The aggregate vector being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
	@Override
	public ValueNode bindExpression(FromList fromList,
									SubqueryList subqueryList,
									List<AggregateNode> aggregateVector) throws StandardException {
		TypeId	operandType;

		bindOperand(fromList, subqueryList, 
				aggregateVector);

		/*
		** Check the type of the operand - this function is allowed only on
		** string value (char and bit) types.
		*/
		operandType = operand.getTypeId();

		switch (operandType.getJDBCTypeId())
		{
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
				case Types.CLOB:
					break;
				case Types.JAVA_OBJECT:
				case Types.OTHER:	
				{
					throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
										methodName,
										operandType.getSQLTypeName());
				}

				default:
					DataTypeDescriptor dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, true, 
							  operand.getTypeCompiler().
								getCastToCharWidth(
									operand.getTypeServices()));
			
					operand =  (ValueNode)
						getNodeFactory().getNode(
							C_NodeTypes.CAST_NODE,
							operand,
							dtd,
							getContextManager());
					
				// DERBY-2910 - Match current schema collation for implicit cast as we do for
				// explicit casts per SQL Spec 6.12 (10)					
			    operand.setCollationUsingCompilationSchema();
			    
				((CastNode) operand).bindCastNodeOnly();
					operandType = operand.getTypeId();
		}

		/*
		** The result type of upper()/lower() is the type of the operand.
		*/

		setType(new DataTypeDescriptor(operandType,
				operand.getTypeServices().isNullable(),
				operand.getTypeCompiler().
					getCastToCharWidth(operand.getTypeServices())
						)
				);
		//Result of upper()/lower() will have the same collation as the   
		//argument to upper()/lower(). 
        setCollationInfo(operand.getTypeServices());

		return this;
	}

	/**
	 * Bind a ? parameter operand of the upper/lower function.
	 *
	 * @exception StandardException		Thrown on error
	 */

	void bindParameter()
			throws StandardException
	{
		/*
		** According to the SQL standard, if bit_length has a ? operand,
		** its type is bit varying with the implementation-defined maximum length
		** for a bit.
		*/

		operand.setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR));
		//collation of ? operand should be same as the compilation schema
		operand.setCollationUsingCompilationSchema();
	}

	/**
	 * This is a length operator node.  Overrides this method
	 * in UnaryOperatorNode for code generation purposes.
	 */
	public String getReceiverInterfaceName() {
	    return ClassName.StringDataValue;
	}
}
