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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl.Format;

import java.sql.Clob;
import java.text.RuleBasedCollator;

/**
 * SQLVarchar represents a VARCHAR value with UCS_BASIC collation.
 *
 * SQLVarchar is mostly the same as SQLChar, so it is implemented as a
 * subclass of SQLChar.  Only those methods with different behavior are
 * implemented here.
 */
public class SQLVarchar
	extends SQLChar
{

	/*
	 * DataValueDescriptor interface.
	 *
	 */

	public String getTypeName()
	{
		return TypeId.VARCHAR_NAME;
	}

	/*
	 * DataValueDescriptor interface
	 */

    /** @see DataValueDescriptor#cloneValue */
    public DataValueDescriptor cloneValue(boolean forceMaterialization)
	{
		try
		{
			SQLVarchar ret = new SQLVarchar(getString());
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
		return new SQLVarchar();
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
		     CollatorSQLVarchar s = new CollatorSQLVarchar(collatorForComparison);
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
		return StoredFormatIds.SQL_VARCHAR_ID;
	}

	/*
	 * constructors
	 */

	public SQLVarchar()
	{
	}

	public SQLVarchar(String val)
	{
		super(val);
	}

	public SQLVarchar(Clob val)
	{
		super(val);
	}

    /**
     * <p>
     * This is a special constructor used when we need to represent a password
     * as a VARCHAR (see DERBY-866). If you need a general-purpose constructor
     * for char[] values and you want to re-use this constructor, make sure to
     * read the comment on the SQLChar( char[] ) constructor.
     * </p>
     */
    public SQLVarchar( char[] val ) { super( val ); }

	/**
	 * Normalization method - this method may be called when putting
	 * a value into a SQLVarchar, for example, when inserting into a SQLVarchar
	 * column.  See NormalizeResultSet in execution.
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
		normalize(desiredType, source.getString());
	}

	protected void normalize(DataTypeDescriptor desiredType, String sourceValue)
		throws StandardException
	{

		int			desiredWidth = desiredType.getMaximumWidth();

		int sourceWidth = sourceValue.length();

		/*
		** If the input is already the right length, no normalization is
		** necessary.
		**
		** It's OK for a Varchar value to be shorter than the desired width.
		** This can happen, for example, if you insert a 3-character Varchar
		** value into a 10-character Varchar column.  Just return the value
		** in this case.
		*/

		if (sourceWidth > desiredWidth) {

			hasNonBlankChars(sourceValue, desiredWidth, sourceWidth);

			/*
			** No non-blank characters will be truncated.  Truncate the blanks
			** to the desired width.
			*/
			sourceValue = sourceValue.substring(0, desiredWidth);
		}

		setValue(sourceValue);
	}


	/*
	 * DataValueDescriptor interface
	 */

	/* @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.VARCHAR_PRECEDENCE;
	}

    /**
     * returns the reasonable minimum amount by
     * which the array can grow . See readExternal.
     * when we know that the array needs to grow by at least
     * one byte, it is not performant to grow by just one byte
     * instead this amount is used to provide a resonable growby size.
     * @return minimum reasonable growby size
     */
    protected final int growBy()
    {
        return RETURN_SPACE_THRESHOLD;  //seems reasonable for a varchar or clob
    }

    public Format getFormat() {
    	return Format.VARCHAR;
    }


    /**
     * Compare two Strings using standard SQL semantics.
     *
     * @param op1               The first String
     * @param op2               The second String
     *
     * @return  -1 - op1 <  op2
     *           0 - op1 == op2
     *           1 - op1 > op2
     */
    protected static int stringCompare(String op1, String op2) {
        /*
        ** By convention, nulls sort High, and null == null
        */
        if (op1 == null || op2 == null) {
            if (op1 != null)    // op2 == null
                return -1;
            if (op2 != null)    // op1 == null
                return 1;
            return 0;           // both null
        }
        return op1.compareTo(op2);
    }

    /**
     * Compare two SQLChars.
     *
     * @exception StandardException     Thrown on error
     */
    protected int stringCompare(StringDataValue char1, StringDataValue char2)
            throws StandardException {
        return stringCompare(char1.getString(), char2.getString());
    }

    /**
     @exception StandardException thrown on error
     */
    public int compare(DataValueDescriptor other) throws StandardException {
        /* Use compare method from dominant type, negating result
         * to reflect flipping of sides.
         */
        if (typePrecedence() < other.typePrecedence()) {
            return - (other.compare(this));
        }
        // stringCompare deals with null as comparable and smallest
        return stringCompare(this, (StringDataValue)other);
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

    public BooleanDataValue equals(DataValueDescriptor left,
                                   DataValueDescriptor right)
            throws StandardException {
        boolean  comparison = stringCompare(left.getString(), right.getString()) == 0;
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

    public BooleanDataValue notEquals(DataValueDescriptor left,
                                      DataValueDescriptor right)
            throws StandardException {
        boolean comparison = stringCompare(left.getString(), right.getString()) != 0;
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

    public BooleanDataValue lessThan(DataValueDescriptor left,
                                     DataValueDescriptor right)
            throws StandardException {
        boolean comparison = stringCompare(left.getString(), right.getString()) < 0;
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

    public BooleanDataValue greaterThan(DataValueDescriptor left,
                                        DataValueDescriptor right)
            throws StandardException
    {
        boolean comparison = stringCompare(left.getString(), right.getString()) > 0;
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

    public BooleanDataValue lessOrEquals(DataValueDescriptor left,
                                         DataValueDescriptor right)
            throws StandardException
    {
        boolean comparison = stringCompare(left.getString(), right.getString()) <= 0;
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

    public BooleanDataValue greaterOrEquals(DataValueDescriptor left,
                                            DataValueDescriptor right)
            throws StandardException
    {
        boolean comparison = stringCompare(left.getString(), right.getString()) >= 0;
        return SQLBoolean.truthValue(left,
                right,
                comparison);
    }

    /* Compared to SQLChar, SQLVarchar honors the trailing space of a string */
    @Override
	public int hashCode()
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(!(this instanceof CollationElementsInterface),
					"SQLVarchar.hashCode() does not work with collation");
		}

		try {
			String str = getString();

			if (str == null)
			{
				return 0;
			} else {
				return str.hashCode();
			}
		}
		catch (StandardException se)
		{
			throw new RuntimeException(se);
		}
	}

}
