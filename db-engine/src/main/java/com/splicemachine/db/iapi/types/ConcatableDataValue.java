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

package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.error.StandardException;

/**
 * The ConcatableDataValue interface corresponds to the
 * SQL 92 string value data type.  It is implemented by
 * datatypes that have a length, and can be concatenated.
 * It is implemented by the character datatypes and the
 * bit datatypes.  
 *
 * The following methods are defined herein:
 *		charLength()
 *
 * The following is defined by the sub classes (bit and char)
 *		concatenate()
 * 
 */
public interface ConcatableDataValue extends DataValueDescriptor, VariableSizeDataValue
{

	/**
	 * The SQL char_length() function.
	 *
	 * @param result	The result of a previous call to this method,
	 *					null if not called yet.
	 *
	 * @return	A NumberDataValue containing the result of the char_length
	 *
	 * @exception StandardException		Thrown on error
	 */
	NumberDataValue charLength(NumberDataValue result)
							throws StandardException;

	/**
	 * substr() function matchs DB2 syntax and behaviour.
	 *
	 * @param start		Start of substr
	 * @param length	Length of substr
	 * @param result	The result of a previous call to this method,
	 *					null if not called yet.
	 * @param maxLen	Maximum length of the result string
	 *
	 * @return	A ConcatableDataValue containing the result of the substr()
	 *
	 * @exception StandardException		Thrown on error
	 */
	ConcatableDataValue substring(
			NumberDataValue start,
			NumberDataValue length,
			ConcatableDataValue result,
			int maxLen,
			boolean isFixedLength)
		throws StandardException;

	/**
	 * The SQL replace() function.
	 *
	 * @param fromStr the substring to be found and replaced
	 *        within this containing string
	 * @param toStr the string to replace the provided substring
	 *        <code>fromStr</code> within this containing string
	 * @param result the result of a previous call to this method,
	 *        or null if not called yet.
	 *
	 * @return ConcatableDataValue containing the result of the replace()
	 *
	 * @exception StandardException if an error occurs
	 */
	ConcatableDataValue replace(
			StringDataValue fromStr,
			StringDataValue toStr,
			ConcatableDataValue result)
            throws StandardException;

	/**
	 * Position in searchFrom of the first occurrence of this.value.
	 * The search begins from position start.  0 is returned if searchFrom does
	 * not contain this.value.  Position 1 is the first character in searchFrom.
	 *
	 * @param searchFrom    - The string or binary string to search from
	 * @param start         - The position to search from in string searchFrom
	 * @param result        - The object to return
	 *
	 * @return  The position in searchFrom the fist occurrence of this.value.
	 *              0 is returned if searchFrom does not contain this.value.
	 * @exception StandardException     Thrown on error
	 */
	NumberDataValue locate(ConcatableDataValue searchFrom,
						   NumberDataValue start,
						   NumberDataValue result)
			throws StandardException;
}
