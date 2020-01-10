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

package com.splicemachine.db.catalog;

import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.StringDataValue;

/**
 * TypeDescriptor represents a type in a system catalog, a
 * persistent type. Examples are columns in tables and parameters
 * for routines. A TypeDescriptor is immutable.
 */

public interface TypeDescriptor
{
	///////////////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	///////////////////////////////////////////////////////////////////////

	/**
	  The return value from getMaximumWidth() for types where the maximum
	  width is unknown.
	 */

	int MAXIMUM_WIDTH_UNKNOWN = -1;
    
    /**
     * Catalog type for nullable INTEGER
     */
    TypeDescriptor INTEGER = DataTypeDescriptor.INTEGER.getCatalogType();

    /**
     * Catalog type for not nullable INTEGER
     */
    TypeDescriptor INTEGER_NOT_NULL =
        DataTypeDescriptor.INTEGER_NOT_NULL.getCatalogType();
    
    /**
     * Catalog type for nullable SMALLINT
     */
    TypeDescriptor SMALLINT = DataTypeDescriptor.SMALLINT.getCatalogType();
    
    /**
     * Catalog type for not nullable INTEGER
     */
    TypeDescriptor SMALLINT_NOT_NULL =
        DataTypeDescriptor.SMALLINT_NOT_NULL.getCatalogType();


    TypeDescriptor DOUBLE = DataTypeDescriptor.DOUBLE.getCatalogType();
	///////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	///////////////////////////////////////////////////////////////////////

	/**
	 * Get the jdbc type id for this type.  JDBC type can be
	 * found in java.sql.Types. 
	 *
	 * @return	a jdbc type, e.g. java.sql.Types.DECIMAL 
	 *
	 * @see java.sql.Types
	 */
	int getJDBCTypeId();

	/**
	  Returns the maximum width of the type.  This may have
	  different meanings for different types.  For example, with char,
	  it means the maximum number of characters, while with int, it
	  is the number of bytes (i.e. 4).

	  @return	the maximum length of this Type; -1 means "unknown/no max length"
	  */
	int			getMaximumWidth();


	/**
	  Returns the maximum width of the type IN BYTES.  This is the
	  maximum number of bytes that could be returned for this type
	  if the corresponding getXXX() method is used.  For example,
	  if we have a CHAR type, then we want the number of bytes
	  that would be returned by a ResultSet.getString() call.

	  @return	the maximum length of this Type IN BYTES;
				-1 means "unknown/no max length"
	  */
	int			getMaximumWidthInBytes();


	/**
	  Returns the number of decimal digits for the type, if applicable.
	 
	  @return	The number of decimal digits for the type.  Returns
	 		zero for non-numeric types.
	  */
	int			getPrecision();


	/**
	  Returns the number of digits to the right of the decimal for
	  the type, if applicable.
	 
	  @return	The number of digits to the right of the decimal for
	 		the type.  Returns zero for non-numeric types.
	  */
	int			getScale();


	/**
	  Gets the nullability that values of this type have.
	  

	  @return	true if values of this type may be null. false otherwise
	  */
	boolean		isNullable();

	/**
	  Gets the name of this type.
	  

	  @return	the name of this type
	  */
	String		getTypeName();


	/**
	  Converts this type descriptor (including length/precision)
	  to a string suitable for appearing in a SQL type specifier.  E.g.
	 
	 			VARCHAR(30)

	  or

	             java.util.Hashtable 
	 
	 
	  @return	String version of type, suitable for running through
	 			a SQL Parser.
	 
	 */
	String		getSQLstring();

	/**
	 * Get the collation type for this type. This api applies only to character
	 * string types. And its return value is valid only if the collation 
	 * derivation  of this type is "implicit" or "explicit". (In Derby 10.3,
	 * collation derivation can't be "explicit". Hence in Derby 10.3, this api
	 * should be used only if the collation derivation is "implicit". 
	 *
	 * @return	collation type which applies to character string types with
	 * collation derivation of "implicit" or "explicit". The possible return
	 * values in Derby 10.3 will be COLLATION_TYPE_UCS_BASIC
     * and COLLATION_TYPE_TERRITORY_BASED.
     * 
     * @see StringDataValue#COLLATION_TYPE_UCS_BASIC
     * @see StringDataValue#COLLATION_TYPE_TERRITORY_BASED
	 * 
	 */
	int getCollationType();

	/**
	 * Return true if this is a Row Multiset type
	  */
	boolean isRowMultiSet();
    
	/**
	 * Return true if this is a user defined type
	  */
	boolean isUserDefinedType();
    
    /**
     * If this catalog type is a row multi-set type
     * then return its array of catalog types.
     * 
     * @return Catalog ypes comprising the row,
     * null if this is not a row type.
     */
	TypeDescriptor[] getRowTypes();

    /**
     * If this catalog type is a row multi-set type
     * then return its array of column names.
     * 
     * @return Column names comprising the row,
     * null if this is not a row type.
     */
	String[] getRowColumnNames();
}

