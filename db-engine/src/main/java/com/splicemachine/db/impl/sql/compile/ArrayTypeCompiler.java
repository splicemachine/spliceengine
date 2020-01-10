/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.TypeCompiler;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.StringDataValue;
import com.splicemachine.db.iapi.types.TypeId;

/**
 * This class implements TypeCompiler for the SQL LOB types.
 *
 */

public class ArrayTypeCompiler extends BaseTypeCompiler
{
        /**
         * Tell whether this type (LOB) can be converted to the given type.
         *
         * @see TypeCompiler#convertible
         */
        public boolean convertible(TypeId otherType, 
								   boolean forDataTypeFunction) {
            return true ;
        }

        /**
         * Tell whether this type (CLOB) is compatible with the given type.
         *
         * @param otherType     The TypeId of the other type.
         */
		public boolean compatible(TypeId otherType)
		{
				return convertible(otherType,false);
		}

	    /**
         * Tell whether this type (LOB) can be stored into from the given type.
         *
         * @param otherType     The TypeId of the other type.
         * @param cf            A ClassFactory
         */

        public boolean storable(TypeId otherType, ClassFactory cf)
        {
            // no automatic conversions at store time--but booleans and string
			// literals (or values of type CHAR/VARCHAR) are STORABLE
            // as clobs, even if the two types can't be COMPARED.
            return true;
        }

        /** @see TypeCompiler#interfaceName */
        public String interfaceName()
        {
            return ClassName.ArrayDataValue;
        }

        /**
         * @see TypeCompiler#getCorrespondingPrimitiveTypeName
         */

        public String getCorrespondingPrimitiveTypeName() {
            int formatId = getStoredFormatIdFromTypeId();
            switch (formatId) {
                case StoredFormatIds.ARRAY_TYPE_ID:  return "java.sql.Array";
                default:
                    if (SanityManager.DEBUG)
                        SanityManager.THROWASSERT("unexpected formatId in getCorrespondingPrimitiveTypeName() - " + formatId);
                    return null;
            }
        }

        /**
         * @see TypeCompiler#getCastToCharWidth
         */
        public int getCastToCharWidth(DataTypeDescriptor dts)
        {
                return dts.getMaximumWidth();
        }

        String nullMethodName() {
            int formatId = getStoredFormatIdFromTypeId();
            switch (formatId) {
                case StoredFormatIds.ARRAY_TYPE_ID:  return "getNullArray";
                default:
                    if (SanityManager.DEBUG)
                        SanityManager.THROWASSERT("unexpected formatId in nullMethodName() - " + formatId);
                    return null;
            }
        }

        String dataValueMethodName()
        {
            int formatId = getStoredFormatIdFromTypeId();
            switch (formatId) {
                case StoredFormatIds.ARRAY_TYPE_ID:  return "getArrayDataValue";
                default:
                    if (SanityManager.DEBUG)
                        SanityManager.THROWASSERT("unexpected formatId in dataValueMethodName() - " + formatId);
                    return null;
                }
        }
        
        /**
         * Push the collation type if it is not COLLATION_TYPE_UCS_BASIC.
         * 
         * @param collationType Collation type of character values.
         * @return true collationType will be pushed, false collationType will be ignored.
         */
        boolean pushCollationForDataValue(int collationType) {
            return collationType != StringDataValue.COLLATION_TYPE_UCS_BASIC;
        }

    /**
     * The caller will have pushed a DataValueFactory and a null or a value
     * of the correct type (interfaceName()). Thus upon entry the
     * stack looks like on of:
     * ...,dvf,ref
     * ...,dvf,null
     *
     * This method then sets up to call the required method
     * on DataValueFactory using the nullMethodName().
     * The value left on the stack will be a DataValueDescriptor
     * of the correct type:
     *
     * ...,dvd
     *
     * @see TypeCompiler#generateNull(MethodBuilder, int)
     */
    @Override
    public void generateNull(MethodBuilder mb, DataTypeDescriptor dtd, LocalField[] localFields) {
        mb.getField(localFields[0]);
        mb.cast(DataValueDescriptor.class.getCanonicalName());
        mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null,
                nullMethodName(),
                interfaceName(),
                2);
    }

}
