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

import com.splicemachine.db.iapi.services.loader.ClassFactory;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.services.io.StoredFormatIds;

import com.splicemachine.db.iapi.types.StringDataValue;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

import com.splicemachine.db.iapi.sql.compile.TypeCompiler;

import com.splicemachine.db.iapi.reference.ClassName;

/**
 * This class implements TypeCompiler for the SQL char datatypes.
 *
 */

public final class CharTypeCompiler extends BaseTypeCompiler
{
	   /**
         * Tell whether this type (char) can be converted to the given type.
         *
		 * @see TypeCompiler#convertible
         */
        public boolean convertible(TypeId otherType, boolean forDataTypeFunction)
        {
            if ( otherType.getBaseTypeId().isAnsiUDT() ) { return false; }
            
			// LONGVARCHAR can only be converted from  character types
			// or CLOB or boolean.
			if (getTypeId().isLongVarcharTypeId())
			{
				return (otherType.isStringTypeId() || otherType.isBooleanTypeId());
			}

			// The double function can convert CHAR and VARCHAR
			if (forDataTypeFunction && otherType.isDoubleTypeId())
				return (getTypeId().isStringTypeId());

			// can't CAST to CHAR and VARCHAR from REAL or DOUBLE
			// or binary types or XML
			// all other types are ok.
			if (otherType.isFloatingPointTypeId() || otherType.isBitTypeId() ||
				otherType.isBlobTypeId() || otherType.isXMLTypeId())
				return false;
						
			return true;
        }
	


	/**
	 * Tell whether this type (char) is compatible with the given type.
	 *
	 * @param otherType     The TypeId of the other type.
	 */
	public boolean compatible(TypeId otherType)
	{
		return (otherType.isStringTypeId() || (otherType.isDateTimeTimeStampTypeId() && !getTypeId().isLongVarcharTypeId()));
		
	}

        /**
         * Tell whether this type (char) can be stored into from the given type.
         *
         * @param otherType     The TypeId of the other type.
         * @param cf            A ClassFactory
         */

        public boolean storable(TypeId otherType, ClassFactory cf)
        {
				// Same rules as cast except we can't assign from numbers
				if (convertible(otherType,false) && 
					!otherType.isBlobTypeId() &&
					!otherType.isNumericTypeId())
						return true;

                /*
                ** If the other type is user-defined, use the java types to determine
                ** assignability.
                */
                return userTypeStorable(getTypeId(), otherType, cf);
        }

        /** @see TypeCompiler#interfaceName */
        public String interfaceName()
        {
                return ClassName.StringDataValue;
        }

        /**
         * @see TypeCompiler#getCorrespondingPrimitiveTypeName
         */

        public String getCorrespondingPrimitiveTypeName()
        {
                /* Only numerics and booleans get mapped to Java primitives */
                return "java.lang.String";
        }

        /**
         * @see TypeCompiler#getCastToCharWidth
         */
        public int getCastToCharWidth(DataTypeDescriptor dts)
        {
                return dts.getMaximumWidth();
        }

        String nullMethodName()
        {
                int formatId = getStoredFormatIdFromTypeId();
                switch (formatId)
                {
                        case StoredFormatIds.CHAR_TYPE_ID:
                                return "getNullChar";

                        case StoredFormatIds.LONGVARCHAR_TYPE_ID:
                                return "getNullLongvarchar";

                        case StoredFormatIds.VARCHAR_TYPE_ID:
                                return "getNullVarchar";

                        default:
                                if (SanityManager.DEBUG)
                                {
                                        SanityManager.THROWASSERT(
                                                "unexpected formatId in nullMethodName() - " + formatId);
                                }
                                return null;
                }
        }

        /**
         * Push the collation type if it is not COLLATION_TYPE_UCS_BASIC.
         * 
         * @param collationType Collation type of character values.
         * @return true collationType will be pushed, false collationType will be ignored.
         */
        boolean pushCollationForDataValue(int collationType)
        {
            return collationType != StringDataValue.COLLATION_TYPE_UCS_BASIC;
        }

        String dataValueMethodName()
        {
                int formatId = getStoredFormatIdFromTypeId();
                switch (formatId)
                {
                        case StoredFormatIds.CHAR_TYPE_ID:
                                return "getCharDataValue";

                        case StoredFormatIds.LONGVARCHAR_TYPE_ID:
                                return "getLongvarcharDataValue";

                        case StoredFormatIds.VARCHAR_TYPE_ID:
                                return "getVarcharDataValue";

                        default:
                                if (SanityManager.DEBUG)
                                {
                                        SanityManager.THROWASSERT(
                                                "unexpected formatId in dataValueMethodName() - " + formatId);
                                }
                                return null;
                }
        }
}
