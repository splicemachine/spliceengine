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

import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.compiler.LocalField;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.reference.ClassName;

import com.splicemachine.db.iapi.services.classfile.VMOpcode;

import java.lang.reflect.Modifier;

import java.sql.Types;

import java.util.List;

/**
 * This node represents a unary DB2 compatible length operator
 *
 */

public final class DB2LengthOperatorNode extends UnaryOperatorNode
{
    
	/**
	 * Initializer for a DB2LengthOperatorNode
	 *
	 * @param operand	The operand of the node
	 */
	public void init(Object	operand)
	{
		super.init( operand, "length", "getDB2Length");
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
        bindOperand( fromList, subqueryList, aggregateVector);

        // This operator is not allowed on XML types.
        TypeId operandType = operand.getTypeId();
        if (operandType.isXMLTypeId()) {
            throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE,
                                    getOperatorString(),
                                    operandType.getSQLTypeName());
        }

        setType( new DataTypeDescriptor( TypeId.getBuiltInTypeId( Types.INTEGER),
                                         operand.getTypeServices().isNullable()));
        return this;
    }

	/**
	 * This is a length operator node.  Overrides this method
	 * in UnaryOperatorNode for code generation purposes.
	 */
	public String getReceiverInterfaceName() {
	    return ClassName.ConcatableDataValue;
	}

    /**
	 * Do code generation for this unary operator.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException
	{
		if (operand == null)
			return;

        int constantLength = getConstantLength();
        // -1 if the length of a non-null operand depends on the data
            
		String resultTypeName = getTypeCompiler().interfaceName();

        mb.pushThis();
		operand.generateExpression(acb, mb);
        mb.upCast( ClassName.DataValueDescriptor);
        mb.push( constantLength);

        /* Allocate an object for re-use to hold the result of the operator */
        LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, resultTypeName);
        mb.getField(field);
        mb.callMethod(VMOpcode.INVOKEVIRTUAL, ClassName.BaseActivation, methodName, resultTypeName, 3);

        /*
        ** Store the result of the method call in the field, so we can re-use
        ** the object.
        */
//        mb.putField(field);
    } // end of generateExpression

    private int getConstantLength( ) throws StandardException
    {
        DataTypeDescriptor typeDescriptor = operand.getTypeServices();
        
        switch( typeDescriptor.getJDBCTypeId())
        {
        case Types.BIGINT:
            return 8;
		case Types.BOOLEAN:
        case Types.BIT:
            return 1;
        case Types.BINARY:
        case Types.CHAR:
            return typeDescriptor.getMaximumWidth();
        case Types.DATE:
            return 4;
        case Types.DECIMAL:
        case Types.NUMERIC:
            return typeDescriptor.getPrecision()/2 + 1;
        case Types.DOUBLE:
            return 8;
        case Types.FLOAT:
        case Types.REAL:
        case Types.INTEGER:
            return 4;
        case Types.SMALLINT:
            return 2;
        case Types.TIME:
            return 3;
        case Types.TIMESTAMP:
            return 10;
        case Types.TINYINT:
            return 1;
        case Types.LONGVARCHAR:
        case Types.VARCHAR:
        case Types.LONGVARBINARY:
        case Types.VARBINARY:
        case Types.BLOB:
            return getConstantNodeLength();
        default:
			return -1;
        }
    } // end of getConstantLength

    private int getConstantNodeLength() throws StandardException
    {
        if( operand instanceof ConstantNode)
            return ((ConstantNode) operand).getValue().getLength();
        return -1;
    }        
}
