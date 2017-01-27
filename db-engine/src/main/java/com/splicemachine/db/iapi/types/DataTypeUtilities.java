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

import com.splicemachine.db.iapi.reference.JDBC30Translation;
import com.splicemachine.db.iapi.reference.JDBC40Translation;

import java.sql.Types;
import java.sql.ResultSetMetaData;

/**
	A set of static utility methods for data types.
 */
public abstract class DataTypeUtilities  {

	/**
		Get the precision of the datatype.
		@param	dtd			data type descriptor
	*/
	public static int getPrecision(DataTypeDescriptor dtd) {
		int typeId = dtd.getTypeId().getJDBCTypeId();

		switch ( typeId )
		{
		case Types.CHAR: // CHAR et alia return their # characters...
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
		case Types.CLOB:
		case Types.BINARY:     	// BINARY types return their # bytes...
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
		case Types.BLOB:
		case JDBC40Translation.SQLXML:
				return dtd.getMaximumWidth();
			case Types.SMALLINT:
				return 5;
			case Types.BOOLEAN:
				return 1;
		}
    	
		return dtd.getPrecision();
	}

	/**
		Get the precision of the datatype, in decimal digits
		This is used by EmbedResultSetMetaData.
		@param	dtd			data type descriptor
	*/
	public static int getDigitPrecision(DataTypeDescriptor dtd) {
		int typeId = dtd.getTypeId().getJDBCTypeId();

		switch ( typeId )
		{
			case Types.FLOAT:
			case Types.DOUBLE:
				return TypeId.DOUBLE_PRECISION_IN_DIGITS;
			case Types.REAL:
				return TypeId.REAL_PRECISION_IN_DIGITS;
			default: return getPrecision(dtd);
		}

	}


	/**
		Is the data type currency.
		@param	dtd			data type descriptor
	*/
	public static boolean isCurrency(DataTypeDescriptor dtd) {
		int typeId = dtd.getTypeId().getJDBCTypeId();

		// Only the NUMERIC and DECIMAL types are currency
		return ((typeId == Types.DECIMAL) || (typeId == Types.NUMERIC));
	}

	/**
		Is the data type case sensitive.
		@param	dtd			data type descriptor
	*/
	public static boolean isCaseSensitive(DataTypeDescriptor dtd) {
		int typeId = dtd.getTypeId().getJDBCTypeId();

		return (typeId == Types.CHAR ||
		          typeId == Types.VARCHAR ||
		          typeId == Types.CLOB ||
		          typeId == Types.LONGVARCHAR ||
		          typeId == JDBC40Translation.SQLXML);
	}
	/**
		Is the data type nullable.
		@param	dtd			data type descriptor
	*/
	public static int isNullable(DataTypeDescriptor dtd) {
		return dtd.isNullable() ?
				ResultSetMetaData.columnNullable :
				ResultSetMetaData.columnNoNulls;
	}

	/**
		Is the data type signed.
		@param	dtd			data type descriptor
	*/
	public static boolean isSigned(DataTypeDescriptor dtd) {
		int typeId = dtd.getTypeId().getJDBCTypeId();

		return ( typeId == Types.INTEGER ||
		     		typeId == Types.FLOAT ||
		     		typeId == Types.DECIMAL ||
		     		typeId == Types.SMALLINT ||
		     		typeId == Types.BIGINT ||
		     		typeId == Types.TINYINT ||
		     		typeId == Types.NUMERIC ||
		     		typeId == Types.REAL ||
		     		typeId == Types.DOUBLE );
	}

	/**
	  *	Gets the display width of a column of a given type.
	  *
	  *	@param	dtd			data type descriptor
	  *
	  *	@return	associated column display width
	  */
	public	static	int getColumnDisplaySize(DataTypeDescriptor dtd)
	{
		int typeId = dtd.getTypeId().getJDBCTypeId();
		int	storageLength = dtd.getMaximumWidth();
		return DataTypeUtilities.getColumnDisplaySize(typeId, storageLength);
	}

	public	static	int getColumnDisplaySize(int typeId, int storageLength)
	{
		int size;
		switch (typeId)
		{
			case Types.TIMESTAMP:
				size = 29;
				break;
			case Types.DATE:
				size = 10;
				break;	
			case Types.TIME:
				size = 8;
				break;
			case Types.INTEGER:
				size = 11;
				break;
			case Types.SMALLINT :
				size = 6;
				break;
			case Types.REAL :
			case Types.FLOAT :
				size = 13;
				break;
			case Types.DOUBLE:
				size = 22;
				break;
			case Types.TINYINT :
				size = 15;
				break;

			case Types.BINARY:
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
            case Types.BLOB:
				size =  2*storageLength;
				if (size < 0)
					size = Integer.MAX_VALUE;
                break;

			case Types.BIGINT:
				size = 20;
				break;
			case Types.BIT:
			case Types.BOOLEAN:
				// Types.BIT == SQL BOOLEAN, so 5 chars for 'false'
				// In JDBC 3.0, Types.BIT or Types.BOOLEAN = SQL BOOLEAN
				size = 5;
				break;
			default: 
				// MaximumWidth is -1 when it is unknown.
				size = (storageLength > 0 ? storageLength : JDBC30Translation.DEFAULT_COLUMN_DISPLAY_SIZE);
				break;
		}
		return size;
	}

    /**
     * Compute the maximum width (column display width) of a decimal or numeric data value,
     * given its precision and scale.
     *
     * @param precision The precision (number of digits) of the data value.
     * @param scale The number of fractional digits (digits to the right of the decimal point).
     *
     * @return The maximum number of chracters needed to display the value.
     */
    public static int computeMaxWidth( int precision, int scale)
    {
	// There are 3 possible cases with respect to finding the correct max
	// width for DECIMAL type.
	// 1. If scale = 0, only sign should be added to precision.
	// 2. scale=precision, 3 should be added to precision for sign, decimal and an additional char '0'.
	// 3. precision > scale > 0, 2 should be added to precision for sign and decimal.
	return (scale ==0) ? (precision +1) : ((scale == precision) ? (precision + 3) : (precision + 2));
    }
}

