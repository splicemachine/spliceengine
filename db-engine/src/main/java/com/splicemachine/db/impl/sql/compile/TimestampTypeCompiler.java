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

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.loader.ClassFactory;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;

import com.splicemachine.db.iapi.sql.compile.TypeCompiler;

import java.sql.Types;
import com.splicemachine.db.iapi.reference.ClassName;

public class TimestampTypeCompiler extends BaseTypeCompiler
{
	/* TypeCompiler methods */	
	/**
	 * User types are convertible to other user types only if
	 * (for now) they are the same type and are being used to
	 * implement some JDBC type.  This is sufficient for
	 * date/time types; it may be generalized later for e.g.
	 * comparison of any user type with one of its subtypes.
	 *
	 * @see TypeCompiler#convertible 
	 *
	 */
	public boolean convertible(TypeId otherType, 
							   boolean forDataTypeFunction)
	{
		if (otherType.isStringTypeId()&& 
			(!otherType.isLongConcatableTypeId()))
		{
			return true;
		}
		
		int otherJDBCTypeId = otherType.getJDBCTypeId();

		/*
		** At this point, we have only date/time.  If
		** same type, convert always ok.
		*/
		if (otherJDBCTypeId == Types.TIMESTAMP)
		{
			return true;
		}

		/*
		** Otherwise, we can convert timestamp to
		** date or time only.
		*/
		return ((otherJDBCTypeId == Types.DATE) ||
				(otherJDBCTypeId == Types.TIME));
	}

        /**
         * Tell whether this type (timestamp) is compatible with the given type.
         *
         * @param otherType     The TypeId of the other type.
         */
	public boolean compatible(TypeId otherType)
	{
		if (otherType.isStringTypeId() &&
			(!otherType.isLongConcatableTypeId()))
		{
			return true;
		}

		/*
		** Both are timestamp datatypes and hence compatible.
		*/
		return (getStoredFormatIdFromTypeId() ==
				otherType.getTypeFormatId());
	}

	/**
	 * User types are storable into other user types that they
	 * are assignable to. The other type must be a subclass of
	 * this type, or implement this type as one of its interfaces.
	 *
	 * Built-in types are also storable into user types when the built-in
	 * type's corresponding Java type is assignable to the user type.
	 *
	 * @param otherType the type of the instance to store into this type.
	 * @param cf		A ClassFactory
	 * @return true if otherType is storable into this type, else false.
	 */
	public boolean storable(TypeId otherType, ClassFactory cf) {
        int otherJDBCTypeId = otherType.getJDBCTypeId();

        return otherJDBCTypeId == Types.TIMESTAMP || (otherJDBCTypeId == Types.CHAR) || (otherJDBCTypeId == Types.VARCHAR) || cf.getClassInspector().assignableTo(otherType.getCorrespondingJavaTypeName(), "java.sql.Timestamp");

    }

	/** @see TypeCompiler#interfaceName */
	public String interfaceName()
	{
		return ClassName.DateTimeDataValue;
	}
			
	/**
	 * @see TypeCompiler#getCorrespondingPrimitiveTypeName
	 */

	public String getCorrespondingPrimitiveTypeName()
	{
		return "java.sql.Timestamp";
	}

	/**
	 * @see TypeCompiler#getCastToCharWidth
	 */
	public int getCastToCharWidth(DataTypeDescriptor dts)
	{
		return 26; // DATE TIME.milliseconds (extra few for good measure)
	}

	public double estimatedMemoryUsage(DataTypeDescriptor dtd)
	{
		return 12.0;
	}
	String nullMethodName()
	{
		return "getNullTimestamp";
	}

    @Override
    public DataTypeDescriptor resolveArithmeticOperation(DataTypeDescriptor leftType, DataTypeDescriptor rightType, String operator) throws StandardException {
        if (operator!= null && (operator.equals("*") || operator.equals("/"))) {
            throw StandardException.newException(SQLState.LANG_DATE_TIME_MULT_DIV_PROHIBITED,
                                                 TypeId.getBuiltInTypeId(Types.TIMESTAMP).toParsableString(leftType));
        }
        DataTypeDescriptor returnType = leftType;
        if (rightType != null && (rightType.getJDBCTypeId() == Types.TIMESTAMP || rightType.getJDBCTypeId() == Types.DATE)) {
            if (operator!= null && operator.equals("+")) {
                throw StandardException.newException(SQLState.LANG_DATE_TIME_ADDITION_PROHIBITED,
                                                     TypeId.getBuiltInTypeId(Types.TIMESTAMP).toParsableString(leftType));
            }
            returnType = DataTypeDescriptor.INTEGER;
        }
        return returnType;
    }
}
