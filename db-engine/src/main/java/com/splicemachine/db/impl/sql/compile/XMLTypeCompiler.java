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

import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.compile.TypeCompiler;

import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

import com.splicemachine.db.iapi.reference.ClassName;

/**
 * This class implements TypeCompiler for the XML type.
 */

public class XMLTypeCompiler extends BaseTypeCompiler
{
    /**
     * Tell whether this type (XML) can be converted to the given type.
     *
     * An XML value can't be converted to any other type, per
     * SQL/XML[2003] 6.3 <cast specification>
     *
     * @see TypeCompiler#convertible
     */
    public boolean convertible(TypeId otherType, 
                            boolean forDataTypeFunction)
    {
        // An XML value cannot be converted to any non-XML type.  If
        // user wants to convert an XML value to a string, then
        // s/he must use the provided SQL/XML serialization operator
        // (namely, XMLSERIALIZE).
        return otherType.isXMLTypeId();
    }

    /**
     * Tell whether this type (XML) is compatible with the given type.
     *
     * @param otherType The TypeId of the other type.
     */
    public boolean compatible(TypeId otherType)
    {
        // An XML value is not compatible (i.e. cannot be "coalesced")
        // into any non-XML type.
        return otherType.isXMLTypeId();
    }

    /**
     * Tell whether this type (XML) can be stored into from the given type.
     * Only XML values can be stored into an XML type, per SQL/XML spec:
     *
     * 4.2.2 XML comparison and assignment
     * Values of XML type are assignable to sites of XML type.
     *
     * @param otherType The TypeId of the other type.
     * @param cf A ClassFactory
     */
    public boolean storable(TypeId otherType, ClassFactory cf)
    {
        // The only type of value that can be stored as XML
        // is an XML value.  Strings are not allowed.  If
        // the user wants to store a string value as XML,
        // s/he must use the provided XML parse operator
        // (namely, XMLPARSE) to parse the string into
        // XML.
        return otherType.isXMLTypeId();
    }

    /**
     * @see TypeCompiler#interfaceName
     */
    public String interfaceName() {
        return ClassName.XMLDataValue;
    }

    /**
     * @see TypeCompiler#getCorrespondingPrimitiveTypeName
     */
    public String getCorrespondingPrimitiveTypeName()
    {
        int formatId = getStoredFormatIdFromTypeId();
        if (formatId == StoredFormatIds.XML_TYPE_ID)
            return "com.splicemachine.db.iapi.types.XML";

        if (SanityManager.DEBUG) {
            SanityManager.THROWASSERT(
                "unexpected formatId in getCorrespondingPrimitiveTypeName(): "
                + formatId);
        }

        return null;
    }

    /**
     * @see TypeCompiler#getCastToCharWidth
     *
     * While it is true XML values can't be cast to char, this method
     * can get called before we finish type checking--so we return a dummy
     * value here and let the type check throw the appropriate error.
     */
    public int getCastToCharWidth(DataTypeDescriptor dts)
    {
        return -1;
    }

    /**
     * @see BaseTypeCompiler#nullMethodName
     */
    String nullMethodName()
    {
        if (SanityManager.DEBUG) {
            if (getStoredFormatIdFromTypeId() != StoredFormatIds.XML_TYPE_ID)
                SanityManager.THROWASSERT(
                "unexpected formatId in nullMethodName(): " + 
                     getStoredFormatIdFromTypeId());
        }
        
        return "getNullXML";
    }

    /**
     * @see BaseTypeCompiler#dataValueMethodName
     */
    protected String dataValueMethodName()
    {
        int formatId = getStoredFormatIdFromTypeId();
        if (formatId == StoredFormatIds.XML_TYPE_ID)
            return "getXMLDataValue";

        if (SanityManager.DEBUG) {
            SanityManager.THROWASSERT(
                "unexpected formatId in dataValueMethodName() - " + formatId);
        }

        return null;
    }
}
