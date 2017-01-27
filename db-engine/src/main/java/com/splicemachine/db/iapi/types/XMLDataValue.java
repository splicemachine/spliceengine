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

package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.error.StandardException;

public interface XMLDataValue extends DataValueDescriptor
{
   /**
     * Method to parse an XML string and, if it's valid,
     * store the _serialized_ version locally and then return
     * this XMLDataValue.
     *
     * @param stringValue The string value to check.
     * @param preserveWS Whether or not to preserve
     *  ignorable whitespace.
     * @param sqlxUtil Contains SQL/XML objects and util
     *  methods that facilitate execution of XML-related
     *  operations
     * @return If 'text' constitutes a valid XML document,
     *  it has been stored in this XML value and this XML
     *  value returned; otherwise, an exception is thrown. 
     * @exception StandardException Thrown on error.
     */
    public XMLDataValue XMLParse(
            StringDataValue stringValue,
            boolean preserveWS,
            SqlXmlUtil sqlxUtil)
        throws StandardException;

    /**
     * The SQL/XML XMLSerialize operator.
     * Serializes this XML value into a string with a user-specified
     * character type, and returns that string via the received
     * StringDataValue (if the received StringDataValue is non-null
     * and of the correct type; else, a new StringDataValue is
     * returned).
     *
     * @param result The result of a previous call to this method,
     *  null if not called yet.
     * @param targetType The string type to which we want to serialize.
     * @param targetWidth The width of the target type.
     * @param targetCollationType The collation type of the target type.
     * @return A serialized (to string) version of this XML object,
     *  in the form of a StringDataValue object.
     * @exception StandardException Thrown on error
     */
    public StringDataValue XMLSerialize(StringDataValue result,
        int targetType, int targetWidth, int targetCollationType) 
    throws StandardException;

    /**
     * The SQL/XML XMLExists operator.
     * Checks to see if evaluation of the query expression contained
     * within the received util object against this XML value returns
     * at least one item. NOTE: For now, the query expression must be
     * XPath only (XQuery not supported) because that's what Xalan
     * supports.
     *
     * @param sqlxUtil Contains SQL/XML objects and util
     *  methods that facilitate execution of XML-related
     *  operations
     * @return True if evaluation of the query expression stored
     *  in sqlxUtil returns at least one node for this XML value;
     *  unknown if the xml value is NULL; false otherwise.
     * @exception StandardException Thrown on error
     */
    public BooleanDataValue XMLExists(SqlXmlUtil sqlxUtil)
		throws StandardException;

    /**
     * Evaluate the XML query expression contained within the received
     * util object against this XML value and store the results into
     * the received XMLDataValue "result" param (assuming "result" is
     * non-null; else create a new XMLDataValue).
     *
     * @param sqlxUtil Contains SQL/XML objects and util methods that
     *  facilitate execution of XML-related operations
     * @param result The result of a previous call to this method; null
     *  if not called yet.
     * @return An XMLDataValue whose content corresponds to the serialized
     *  version of the results from evaluation of the query expression.
     *  Note: this XMLDataValue may not be storable into Derby XML
     *  columns.
     * @exception Exception thrown on error (and turned into a
     *  StandardException by the caller).
     */
    public XMLDataValue XMLQuery(SqlXmlUtil sqlxUtil, XMLDataValue result)
		throws StandardException;

    /* ****
     * Helper classes and methods.
     * */

    /**
     * Set this XML value's qualified type.
     */
    public void setXType(int xtype);

    /**
     * Retrieve this XML value's qualified type.
     */
    public int getXType();

    /**
     * Take note of the fact this XML value represents an XML
     * sequence that has one or more top-level attribute nodes.
     */
    public void markAsHavingTopLevelAttr();

    /**
     * Return whether or not this XML value represents a sequence
     * that has one or more top-level attribute nodes.
     */
    public boolean hasTopLevelAttr();
}
