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

import java.text.RuleBasedCollator;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.io.StoredFormatIds;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.util.StringUtil;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl.Format;

/**
 * SQLLongvarchar represents a LONG VARCHAR value with UCS_BASIC collation.
 *
 * SQLLongvarchar is mostly the same as SQLVarchar, so it is implemented as a
 * subclass of SQLVarchar.  Only those methods with different behavior are
 * implemented here.
 */
public class SQLLongvarchar
	extends SQLVarchar
{
	/*
	 * DataValueDescriptor interface.
	 *
	 * These are actually all implemented in the super-class, but we need
	 * to duplicate some of them here so they can be called by byte-code
	 * generation, which needs to know the class the method appears in.
	 */

	public String getTypeName()
	{
		return TypeId.LONGVARCHAR_NAME;
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#cloneValue */
	public DataValueDescriptor cloneValue(boolean forceMaterialization)
	{
		try
		{
			return new SQLLongvarchar(getString());
		}
		catch (StandardException se)
		{
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Unexpected exception", se);
			return null;
		}
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 *
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLLongvarchar();
	}

	/** @see StringDataValue#getValue(RuleBasedCollator) */
	public StringDataValue getValue(RuleBasedCollator collatorForComparison)
	{
		if (collatorForComparison == null)
		{//null collatorForComparison means use UCS_BASIC for collation
		    return this;			
		} else {
			//non-null collatorForComparison means use collator sensitive
			//implementation of SQLLongvarchar
		     CollatorSQLLongvarchar s = new CollatorSQLLongvarchar(collatorForComparison);
		     s.copyState(this);
		     return s;
		}
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */

	/**
		Return my format identifier.

		@see com.splicemachine.db.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_LONGVARCHAR_ID;
	}

	/*
	 * constructors
	 */

	public SQLLongvarchar()
	{
	}

	public SQLLongvarchar(String val)
	{
		super(val);
	}

	protected void normalize(DataTypeDescriptor desiredType, String sourceValue)
		throws StandardException
	{
		//bug 5592 - for sql long varchar, any truncation is disallowed ie even the trailing blanks can't be truncated
		if (sourceValue.length() > desiredType.getMaximumWidth())
			throw StandardException.newException(SQLState.LANG_STRING_TRUNCATION, getTypeName(), StringUtil.formatForPrint(sourceValue), String.valueOf(desiredType.getMaximumWidth()));

		setValue(sourceValue);
	}

	/**
	 * @see StringDataValue#concatenate
	 *
	 * @exception StandardException		Thrown on error
	 */
	public StringDataValue concatenate(
				StringDataValue leftOperand,
				StringDataValue rightOperand,
				StringDataValue result)
		throws StandardException
	{
		result = super.concatenate(leftOperand, rightOperand, result);

		//bug 5600 - according to db2 concatenation documentation, for compatibility with previous versions, there is no automatic
		//escalation of results involving LONG data types to LOB data types. For eg, concatenation of a CHAR(200) value and a
		//completely full LONG VARCHAR value would result in an error rather than in a promotion to a CLOB data type

		//need to check for concatenated string for null value
		if ((result.getString() != null) && (result.getString().length() > TypeId.LONGVARCHAR_MAXWIDTH))
			throw StandardException.newException(SQLState.LANG_CONCAT_STRING_OVERFLOW, "CONCAT", String.valueOf(TypeId.LONGVARCHAR_MAXWIDTH));

		return result;
	}

	/*
	 * DataValueDescriptor interface
	 */

	/* @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.LONGVARCHAR_PRECEDENCE;
	}
	
	public Format getFormat() {
		return Format.LONGVARCHAR;
	}
}
