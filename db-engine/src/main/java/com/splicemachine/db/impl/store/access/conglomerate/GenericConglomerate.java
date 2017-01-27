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

package com.splicemachine.db.impl.store.access.conglomerate;

import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;

import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataType;
import com.splicemachine.db.iapi.types.StringDataValue;

import java.sql.ResultSet;
import java.sql.SQLException;

/**

A class that implements the methods shared across all implementations of
the Conglomerate interface.

**/

public abstract class GenericConglomerate 
    extends DataType implements Conglomerate
{

    /**************************************************************************
     * Public Methods implementing DataValueDescriptor interface.
     **************************************************************************
     */

	/**
	 * Gets the length of the data value.  The meaning of this is
	 * implementation-dependent.  For string types, it is the number of
	 * characters in the string.  For numeric types, it is the number of
	 * bytes used to store the number.  This is the actual length
	 * of this value, not the length of the type it was defined as.
	 * For example, a VARCHAR value may be shorter than the declared
	 * VARCHAR (maximum) length.
	 *
	 * @return	The length of the data value
	 *
	 * @exception StandardException   On error
     * 
     * @see com.splicemachine.db.iapi.types.DataValueDescriptor#getLength
	 */
	public int	getLength() 
        throws StandardException
    {
        throw(StandardException.newException(
                SQLState.HEAP_UNIMPLEMENTED_FEATURE));
    }
	/**
	 * Gets the value in the data value descriptor as a String.
	 * Throws an exception if the data value is not a string.
	 *
	 * @return	The data value as a String.
	 *
	 * @exception StandardException   Thrown on error
     *
     * @see com.splicemachine.db.iapi.types.DataValueDescriptor#getString
	 */
	public String	getString() throws StandardException
    {
        throw(StandardException.newException(
                SQLState.HEAP_UNIMPLEMENTED_FEATURE));
    }

	/**
	 * Gets the value in the data value descriptor as a Java Object.
	 * The type of the Object will be the Java object type corresponding
	 * to the data value's SQL type. JDBC defines a mapping between Java
	 * object types and SQL types - we will allow that to be extended
	 * through user type definitions. Throws an exception if the data
	 * value is not an object (yeah, right).
	 *
	 * @return	The data value as an Object.
	 *
	 * @exception StandardException   Thrown on error
     *
     * @see com.splicemachine.db.iapi.types.DataValueDescriptor#getObject
	 */
	public Object	getObject() throws StandardException
    {
        return(this);
    }

	/**
     * @see com.splicemachine.db.iapi.types.DataValueDescriptor#cloneValue
	 */
	public DataValueDescriptor cloneValue(boolean forceMaterialization)
    {
        if (SanityManager.DEBUG)
            SanityManager.THROWASSERT("Not implemented!.");

        return(null);
    }

	/**
	 * Get a new null value of the same type as this data value.
	 *
     * @see com.splicemachine.db.iapi.types.DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
    {
        if (SanityManager.DEBUG)
            SanityManager.THROWASSERT("Not implemented!.");

        return(null);
    }

	/**
	 * Set the value based on the value for the specified DataValueDescriptor
	 * from the specified ResultSet.
	 *
	 * @param resultSet		The specified ResultSet.
	 * @param colNumber		The 1-based column # into the resultSet.
	 * @param isNullable	Whether or not the column is nullable
	 *						(No need to call wasNull() if not)
	 * 
	 * @exception StandardException		Thrown on error
	 * @exception SQLException		Error accessing the result set
     *
     * @see com.splicemachine.db.iapi.types.DataValueDescriptor#setValueFromResultSet
	 */
	public void setValueFromResultSet(
    ResultSet   resultSet, 
    int         colNumber,
    boolean     isNullable)
		throws StandardException, SQLException
    {
        throw(StandardException.newException(
                SQLState.HEAP_UNIMPLEMENTED_FEATURE));
    }


	/**
	 * Set the value of this DataValueDescriptor from another.
	 *
	 * @param theValue	The Date value to set this DataValueDescriptor to
	 *
     * @see com.splicemachine.db.iapi.types.DataValueDescriptor#setValue
	 */
	protected void setFrom(DataValueDescriptor theValue) 
        throws StandardException
    {
        throw(StandardException.newException(
                SQLState.HEAP_UNIMPLEMENTED_FEATURE));
    }

	/**
	 * Get the SQL name of the datatype
	 *
	 * @return	The SQL name of the datatype
     *
     * @see com.splicemachine.db.iapi.types.DataValueDescriptor#getTypeName
	 */
	public String	getTypeName()
    {
        if (SanityManager.DEBUG)
            SanityManager.THROWASSERT("Not implemented!.");

        return(null);
    }

	/**
	 * Compare this Orderable with a given Orderable for the purpose of
	 * index positioning.  This method treats nulls as ordered values -
	 * that is, it treats SQL null as equal to null and less than all
	 * other values.
	 *
	 * @param other		The Orderable to compare this one to.
	 *
	 * @return  <0 - this Orderable is less than other.
	 * 			 0 - this Orderable equals other.
	 *			>0 - this Orderable is greater than other.
     *
     *			The code should not explicitly look for -1, or 1.
	 *
	 * @exception StandardException		Thrown on error
     *
     * @see DataValueDescriptor#compare
	 */
	public int compare(DataValueDescriptor other) 
        throws StandardException
	{
        throw(StandardException.newException(
                SQLState.HEAP_UNIMPLEMENTED_FEATURE));
	}

    /**
     * Tells if there are columns with collations (other than UCS BASIC) in the
     * given list of collation ids.
     *
     * @param collationIds collation ids for the conglomerate columns
     * @return {@code true} if a collation other than UCS BASIC was found.
     */
    public static boolean hasCollatedColumns(int[] collationIds) {
        for (int i=0; i < collationIds.length; i++) {
            if (collationIds[i] != StringDataValue.COLLATION_TYPE_UCS_BASIC) {
                return true;
            }
        }
        return false;
    }
}
