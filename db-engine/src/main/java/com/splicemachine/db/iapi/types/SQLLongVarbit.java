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
import com.splicemachine.db.iapi.reference.Limits;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl.Format;

/**
 * SQLLongVarbit represents the SQL type LONG VARCHAR FOR BIT DATA
 * It is an extension of SQLVarbit and is virtually indistinguishable
 * other than normalization.
 */
public class SQLLongVarbit extends SQLVarbit
{

	public String getTypeName()
	{
		return TypeId.LONGVARBIT_NAME;
	}

	/**
	 * Return max memory usage for a SQL LongVarbit
	 */
	int getMaxMemoryUsage()
	{
		return Limits.DB2_LONGVARCHAR_MAXWIDTH;
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLLongVarbit();
	}

	/**
		Return my format identifier.

		@see com.splicemachine.db.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId()
	{
		return StoredFormatIds.SQL_LONGVARBIT_ID;
	}


	/*
	 * Orderable interface
	 */


	/*
	 * Column interface
	 */


	/*
	 * class interface
	 */

	/*
	 * constructors
	 */
	public SQLLongVarbit()
	{
	}

	public SQLLongVarbit(byte[] val)
	{
		super(val);
	}

	/**
	 * Normalization method - this method may be called when putting
	 * a value into a SQLVarbit, for example, when inserting into a SQLVarbit
	 * column.  See NormalizeResultSet in execution.
	 *
	 * This overrides SQLBit -- the difference is that we don't
	 * expand SQLVarbits to fit the target.
	 * 
	 * @param desiredType	The type to normalize the source column to
	 * @param source		The value to normalize
	 *
	 *
	 * @exception StandardException				Thrown for null into
	 *											non-nullable column, and for
	 *											truncation error
	 */

	public void normalize(
				DataTypeDescriptor desiredType,
				DataValueDescriptor source)
					throws StandardException
	{
		if (source instanceof SQLLongVarbit) {
			// avoid creating an object in memory if a matching type.
			// this may be a stream.
			SQLLongVarbit other = (SQLLongVarbit) source;
			this.stream = other.stream;
			this.dataValue = other.dataValue;
		}
		else
			setValue(source.getBytes());
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.LONGVARBIT_PRECEDENCE;
	}
	
	public Format getFormat() {
		return Format.LONGVARBIT;
	}
	
}
