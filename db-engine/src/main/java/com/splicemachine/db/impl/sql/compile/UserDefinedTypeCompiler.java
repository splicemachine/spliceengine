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

import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.sql.compile.TypeCompiler;
import com.splicemachine.db.catalog.types.UserDefinedTypeIdImpl;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.reference.ClassName;

public class UserDefinedTypeCompiler extends BaseTypeCompiler
{
	/* TypeCompiler methods */

	/**
	 * Right now, casting is not allowed from one user defined type
     * to another.
	 *
	 * @param otherType 
	 * @param forDataTypeFunction
	 * @return true if otherType is convertible to this type, else false.
	 * 
	 *@see TypeCompiler#convertible
	 */
	public boolean convertible(TypeId otherType, boolean forDataTypeFunction)
	{
        if ( getTypeId().getBaseTypeId().isAnsiUDT() )
        {
            if ( !otherType.getBaseTypeId().isAnsiUDT() ) { return false; }
            
            UserDefinedTypeIdImpl thisTypeID = (UserDefinedTypeIdImpl) getTypeId().getBaseTypeId();
            UserDefinedTypeIdImpl thatTypeID = (UserDefinedTypeIdImpl) otherType.getBaseTypeId();
            
            return thisTypeID.getSQLTypeName().equals( thatTypeID.getSQLTypeName() );
        }
        
		/*
		** We are a non-ANSI user defined type, we are
		** going to have to let the client find out
		** the hard way.
		*/
		return true;
	}

	 /** @see TypeCompiler#compatible */
	public boolean compatible(TypeId otherType)
	{
		return convertible(otherType, false);
	}

	/**
     * ANSI UDTs can only be stored into values of exactly their own
     * type. This restriction can be lifted when we implement the
     * ANSI subclassing clauses.
     *
	 * Old-style User types are storable into other user types that they
	 * are assignable to. The other type must be a subclass of
	 * this type, or implement this type as one of its interfaces.
	 *
	 * @param otherType the type of the instance to store into this type.
	 * @param cf		A ClassFactory
	 * @return true if otherType is storable into this type, else false.
	 */
	public boolean storable(TypeId otherType, ClassFactory cf)
	{
        if ( !otherType.isUserDefinedTypeId() ) { return false; }

        UserDefinedTypeIdImpl thisTypeID = (UserDefinedTypeIdImpl) getTypeId().getBaseTypeId();
        UserDefinedTypeIdImpl thatTypeID = (UserDefinedTypeIdImpl) otherType.getBaseTypeId();

        if ( thisTypeID.isAnsiUDT() != thatTypeID.isAnsiUDT() ) { return false; }

        if ( thisTypeID.isAnsiUDT() )
        {
            return thisTypeID.getSQLTypeName().equals( thatTypeID.getSQLTypeName() );
        }
        
		return cf.getClassInspector().assignableTo(
			   otherType.getCorrespondingJavaTypeName(),
			   getTypeId().getCorrespondingJavaTypeName());
	}

	/** @see TypeCompiler#interfaceName */
	public String interfaceName()
	{
		return ClassName.UserDataValue;
	}
			
	/**
	 * @see TypeCompiler#getCorrespondingPrimitiveTypeName
	 */

	public String getCorrespondingPrimitiveTypeName()
	{
		return getTypeId().getCorrespondingJavaTypeName();
	}

	/**
	 * @see TypeCompiler#getCastToCharWidth
	 */
	public int getCastToCharWidth(DataTypeDescriptor dts)
	{
		// This is the maximum maximum width for user types
		return -1;
	}

	String nullMethodName()
	{
		return "getNullObject";
	}

	public void generateDataValue(MethodBuilder mb, int collationType,
			LocalField field)
	{
		// cast the value to an object for method resolution
		mb.upCast("java.lang.Object");

		super.generateDataValue(mb, collationType, field);
	}

    @Override
	public void generateNull(MethodBuilder mb, DataTypeDescriptor dtd, LocalField[] fields) {
		int argCount;
        try {
            LanguageConnectionContext lcc = (LanguageConnectionContext)
                    ContextService.getContextOrNull(LanguageConnectionContext.CONTEXT_ID);
            ClassFactory cf = lcc.getLanguageConnectionFactory().getClassFactory();
            String typeName = getCorrespondingPrimitiveTypeName();
            if (typeName.contains(".")) {
                Class thisClazz = cf.loadApplicationClass(typeName);
                Class UDTBaseClazz = cf.loadApplicationClass(ClassName.UDTBase);
                if (UDTBaseClazz.isAssignableFrom(thisClazz)) {
                    // If this is a user-defined data type or aggregator, new an instance of this type
                    // so that splice can distinguish it with other classes that are serialize/deserialize by Kryo.
                    mb.pop();
                    mb.pushNewStart(typeName);
                    mb.pushNewComplete(0);
                    mb.upCast("java.lang.Object");
                }
            }
            if (pushCollationForDataValue(dtd.getCollationType())) {
                mb.push(dtd.getCollationType());
                argCount = 2;
            } else
                argCount = 1;

            mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null,
                    nullMethodName(),
                    interfaceName(),
                    argCount);
        } catch (Exception e) {
            throw new RuntimeException(e.getCause());
        }
	}

}
