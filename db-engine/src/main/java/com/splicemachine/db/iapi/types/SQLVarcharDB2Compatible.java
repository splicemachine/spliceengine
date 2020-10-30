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

import com.splicemachine.db.catalog.types.CatalogMessage;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

import java.sql.Clob;
import java.text.RuleBasedCollator;

public class SQLVarcharDB2Compatible extends SQLVarchar
{
	/*
	 * DataValueDescriptor interface
	 */

    /** @see DataValueDescriptor#cloneValue */
    public DataValueDescriptor cloneValue(boolean forceMaterialization)
	{
		try
		{
			SQLVarcharDB2Compatible ret = new SQLVarcharDB2Compatible(getString());
			ret.setSqlCharSize(super.getSqlCharSize());
			return ret;
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
		return new SQLVarcharDB2Compatible();
	}

	/** @see StringDataValue#getValue(RuleBasedCollator) */
	public StringDataValue getValue(RuleBasedCollator collatorForComparison)
	{
		if (collatorForComparison == null)
		{//null collatorForComparison means use UCS_BASIC for collation
		    return this;
		} else {
			//non-null collatorForComparison means use collator sensitive
			//implementation of SQLVarchar
		     CollatorSQLVarcharDB2Compatible s = new CollatorSQLVarcharDB2Compatible(collatorForComparison);
		     s.copyState(this);
		     return s;
		}
	}

	/*
	 * constructors
	 */

	public SQLVarcharDB2Compatible()
	{
	}

	public SQLVarcharDB2Compatible(String val)
	{
		super(val);
	}

	public SQLVarcharDB2Compatible(Clob val)
	{
		super(val);
	}

    public SQLVarcharDB2Compatible( char[] val ) { super( val ); }

    public SQLVarcharDB2Compatible(CatalogMessage.SQLChar sqlChar) {
		init(sqlChar);
	}

	/*
	 * DataValueDescriptor interface
	 */

    /**
     * Compare two SQLChars.
     *
     * @exception StandardException     Thrown on error
     */
    protected int stringCompare(StringDataValue char1, StringDataValue char2)
            throws StandardException {
        return super.stringCompareDB2Compatible(char1, char2);
    }

    protected int compareStrings(String op1, String op2) {
		return SQLChar.stringCompare(op1, op2);
	}

    /**
     * The = operator as called from the language module, as opposed to
     * the storage module.
     *
     * @param left          The value on the left side of the =
     * @param right         The value on the right side of the =
     *
     * @return  A SQL boolean value telling whether the two parameters are equal
     *
     * @exception StandardException     Thrown on error
     */
    @Override
    public BooleanDataValue equals(DataValueDescriptor left,
                                   DataValueDescriptor right)
            throws StandardException {
        boolean  comparison = compareStrings(left.getString(), right.getString()) == 0;
        return SQLBoolean.truthValue(left,
                right,
                comparison);
    }

    /**
     * The <> operator as called from the language module, as opposed to
     * the storage module.
     *
     * @param left          The value on the left side of the <>
     * @param right         The value on the right side of the <>
     *
     * @return  A SQL boolean value telling whether the two parameters
     * are not equal
     *
     * @exception StandardException     Thrown on error
     */
    @Override
    public BooleanDataValue notEquals(DataValueDescriptor left,
                                      DataValueDescriptor right)
            throws StandardException {
        boolean comparison = compareStrings(left.getString(), right.getString()) != 0;
        return SQLBoolean.truthValue(left,
                right,
                comparison);
    }

    /**
     * The < operator as called from the language module, as opposed to
     * the storage module.
     *
     * @param left          The value on the left side of the <
     * @param right         The value on the right side of the <
     *
     * @return  A SQL boolean value telling whether the first operand is
     *          less than the second operand
     *
     * @exception StandardException     Thrown on error
     */
    @Override
    public BooleanDataValue lessThan(DataValueDescriptor left,
                                     DataValueDescriptor right)
            throws StandardException {
        boolean comparison = compareStrings(left.getString(), right.getString()) < 0;
        return SQLBoolean.truthValue(left,
                right,
                comparison);
    }

    /**
     * The > operator as called from the language module, as opposed to
     * the storage module.
     *
     * @param left          The value on the left side of the >
     * @param right         The value on the right side of the >
     *
     * @return  A SQL boolean value telling whether the first operand is
     *          greater than the second operand
     *
     * @exception StandardException     Thrown on error
     */
    @Override
    public BooleanDataValue greaterThan(DataValueDescriptor left,
                                        DataValueDescriptor right)
            throws StandardException
    {
        boolean comparison = compareStrings(left.getString(), right.getString()) > 0;
        return SQLBoolean.truthValue(left,
                right,
                comparison);
    }

    /**
     * The <= operator as called from the language module, as opposed to
     * the storage module.
     *
     * @param left          The value on the left side of the <=
     * @param right         The value on the right side of the <=
     *
     * @return  A SQL boolean value telling whether the first operand is
     *          less than or equal to the second operand
     *
     * @exception StandardException     Thrown on error
     */
    @Override
    public BooleanDataValue lessOrEquals(DataValueDescriptor left,
                                         DataValueDescriptor right)
            throws StandardException
    {
        boolean comparison = compareStrings(left.getString(), right.getString()) <= 0;
        return SQLBoolean.truthValue(left,
                right,
                comparison);
    }

    /**
     * The >= operator as called from the language module, as opposed to
     * the storage module.
     *
     * @param left          The value on the left side of the >=
     * @param right         The value on the right side of the >=
     *
     * @return  A SQL boolean value telling whether the first operand is
     *          greater than or equal to the second operand
     *
     * @exception StandardException     Thrown on error
     */
    @Override
    public BooleanDataValue greaterOrEquals(DataValueDescriptor left,
                                            DataValueDescriptor right)
            throws StandardException
    {
        boolean comparison = compareStrings(left.getString(), right.getString()) >= 0;
        return SQLBoolean.truthValue(left,
                right,
                comparison);
    }


}
