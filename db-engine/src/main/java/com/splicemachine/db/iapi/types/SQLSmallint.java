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

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.ArrayInputStream;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.io.Storable;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.cache.ClassSize;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl.Format;
import com.yahoo.sketches.theta.UpdateSketch;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

/**
 * SQLSmallint satisfies the DataValueDescriptor
 * interfaces (i.e., OrderableDataType). It implements a smallint column, 
 * e.g. for storing a column value; it can be specified
 * when constructed to not allow nulls. Nullability cannot be changed
 * after construction, as it affects the storage size and mechanism.
 * <p>
 * Because OrderableDataType is a subtype of ValueColumn,
 * SQLSmallint can play a role in either a ValueColumn/Row
 * or a OrderableDataType/Row, interchangeably.
 * <p>
 * We assume the store has a flag for nullness of the value,
 * and simply return a 0-length array for the stored form
 * when the value is null.
 * <p>
 * PERFORMANCE: There are likely alot of performance improvements
 * possible for this implementation -- it new's Short
 * more than it probably wants to.
 */
public final class SQLSmallint
	extends NumberDataType
{
	/*
	 * DataValueDescriptor interface
	 * (mostly implemented in DataType)
	 */


        // JDBC is lax in what it permits and what it
	// returns, so we are similarly lax

	/** 
	 * @see DataValueDescriptor#getInt 
	 */
	public int	getInt()
	{
		return (int) value;
	}

	/** 
	 * @exception StandardException thrown on failure to convert
	 * @see DataValueDescriptor#getByte 
	 */
	public byte	getByte() throws StandardException
	{
		if (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE)
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "TINYINT");
		return (byte) value;
	}

	/** 
	 * @see DataValueDescriptor#getShort 
	 */
	public short	getShort()
	{
		return value;
	}

	/** 
	 * @see DataValueDescriptor#getLong 
	 */
	public long	getLong()
	{
		return (long) value;
	}

	/** 
	 * @see DataValueDescriptor#getFloat 
	 */
	public float	getFloat()
	{
		return (float) value;
	}

	/** 
	 * @see DataValueDescriptor#getDouble 
	 */
	public double	getDouble()
	{
		return (double) value;
	}

    // for lack of a specification: 0 or null is false,
    // all else is true
	/** 
	 * @see DataValueDescriptor#getBoolean 
	 */
	public boolean	getBoolean()
	{
		return (value != 0);
	}

	/** 
	 * @see DataValueDescriptor#getString 
	 */
	public String	getString()
	{
		if (isNull())
			return null;
		else
			return Short.toString(value);
	}

	/** 
	 * @see DataValueDescriptor#getLength 
	 */
	public int	getLength()
	{
		return SMALLINT_LENGTH;
	}

	/** 
	 * @see DataValueDescriptor#getObject 
	 */
	public Object	getObject() 
	{
		if (isNull())
			return null;
		else
			return (int) value;
	}

	// this is for DataType's error generator
	public String getTypeName()
	{
		return TypeId.SMALLINT_NAME;
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */


	/**
		Return my format identifier.

		@see com.splicemachine.db.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_SMALLINT_ID;
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeBoolean(isNull);
		out.writeShort(value);
	}

	/** @see java.io.Externalizable#readExternal */
	public void readExternalFromArray(ArrayInputStream in) throws IOException {
		isNull = in.readBoolean();
		value = in.readShort();
	}
	public void readExternal(ObjectInput in) throws IOException {
		isNull = in.readBoolean();
		value = in.readShort();
	}

	/**
	 * @see Storable#restoreToNull
	 *
	 */
	public void restoreToNull()
	{
		value = 0;
		isNull = true;
	}


	/** @exception StandardException		Thrown on error */
	protected int typeCompare(DataValueDescriptor arg) throws StandardException
	{
		/* neither are null, get the value */

		/* Do comparisons with ints to avoid overflow problems */
		int thisValue = this.getInt();
		int otherValue = arg.getInt();
		if (thisValue == otherValue)
			return 0;
		else if (thisValue > otherValue)
			return 1;
		else
			return -1;
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#cloneValue */
	public DataValueDescriptor cloneValue(boolean forceMaterialization)
	{
		return new SQLSmallint(value, isNull);
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLSmallint();
	}

	/** 
	 * @see DataValueDescriptor#setValueFromResultSet 
	 *
	 * @exception SQLException		Thrown on error
	 */
	public void setValueFromResultSet(ResultSet resultSet, int colNumber,
									  boolean isNullable)
		throws SQLException
	{
		try {
			value = resultSet.getShort(colNumber);
			isNull = (isNullable && resultSet.wasNull());
		} catch (SQLException selq) {
			int i = resultSet.getInt(colNumber);
			value = (short) i;
			isNull = false;

		}
	}
	/**
		Set the value into a PreparedStatement.

		@exception SQLException Error setting value in PreparedStatement
	*/
	public final void setInto(PreparedStatement ps, int position) throws SQLException {

		if (isNull()) {
			ps.setNull(position, java.sql.Types.SMALLINT);
			return;
		}

		ps.setShort(position, value);
	}
	/**
		Set this value into a ResultSet for a subsequent ResultSet.insertRow
		or ResultSet.updateRow. This method will only be called for non-null values.

		@exception SQLException thrown by the ResultSet object
		@exception StandardException thrown by me accessing my value.
	*/
	public final void setInto(ResultSet rs, int position) throws SQLException, StandardException {
		rs.updateShort(position, value);
	}


	/*
	 * class interface
	 */

	/*
	 * constructors
	 */

	/**
		No-arg constructor, required by Formattable.
    // This constructor also gets used when we are
    // allocating space for a short.
	*/
	public SQLSmallint() 
	{
		isNull = true;
	}

	public SQLSmallint(short val)
	{
		setValue(val);
	}

	public SQLSmallint(int val) throws StandardException
	{
		setValue(val);
	}


	/* This constructor gets used for the cloneValue() method */
	private SQLSmallint(short val, boolean isNull) {
		setValue(val);
		this.isNull = isNull;
	}

	public SQLSmallint(Short obj) {
		if (isNull = (obj == null))
			;
		else
			setValue(obj.shortValue());
	}

	/**
		@exception StandardException thrown if string not accepted
	 */
	public void setValue(String theValue)
		throws StandardException
	{
		if (theValue == null)
		{
			value = 0;
			isNull = true;
		}
		else
		{
		    try {
		        value = Short.valueOf(theValue.trim());
			} catch (NumberFormatException nfe) {
			    throw invalidFormat();
			}
			isNull = false;
		}

	}

	public void setValue(short theValue)
	{
		value = theValue;
		isNull = false;
	}

	public void setValue(byte theValue)
	{
		value = theValue;
		isNull = false;
	}

	/**
		@exception StandardException if outsideRangeForSmallint
	 */
	public void setValue(int theValue) throws StandardException
	{
		if (theValue > Short.MAX_VALUE || theValue < Short.MIN_VALUE)
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "SMALLINT");
		value = (short)theValue;
		isNull = false;
	}

	/**
		@exception StandardException if outsideRangeForSmallint
	 */
	public void setValue(long theValue) throws StandardException
	{
		if (theValue > Short.MAX_VALUE || theValue < Short.MIN_VALUE)
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "SMALLINT");
		value = (short)theValue;
		isNull = false;
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(float theValue) throws StandardException
	{
		theValue = NumberDataType.normalizeREAL(theValue);

		if (theValue > Short.MAX_VALUE || theValue < Short.MIN_VALUE)
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "SMALLINT");

		float floorValue = (float)Math.floor(theValue);

		value = (short)floorValue;
		isNull = false;
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(double theValue) throws StandardException
	{
		theValue = NumberDataType.normalizeDOUBLE(theValue);

		if (theValue > Short.MAX_VALUE || theValue < Short.MIN_VALUE)
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "SMALLINT");

		double floorValue = Math.floor(theValue);

		value = (short)floorValue;
		isNull = false;
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 */
	public void setValue(boolean theValue)
	{
		value = theValue?(short)1:(short)0;
		isNull = false;
	}

	/* This is constructor used fo PyStoredProcedureResultSetFactory */
	public void setValue(Integer theValue) throws StandardException{
		if(theValue==null){
			restoreToNull();
			return;
		}
		setValue(theValue.intValue());
	}

	protected void setFrom(DataValueDescriptor theValue) throws StandardException {

		setValue(theValue.getShort());
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.SMALLINT_PRECEDENCE;
	}


	/*
	** SQL Operators
	*/

	/**
	 * The = operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the =
	 * @param right			The value on the right side of the =
	 *
	 * @return	A SQL boolean value telling whether the two parameters are equal
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue equals(DataValueDescriptor left,
							DataValueDescriptor right)
			throws StandardException
	{
		return SQLBoolean.truthValue(left,
									 right,
									 left.getShort() == right.getShort());
	}

	/**
	 * The <> operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the <>
	 * @param right			The value on the right side of the <>
	 *
	 * @return	A SQL boolean value telling whether the two parameters
	 *			are not equal
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue notEquals(DataValueDescriptor left,
							DataValueDescriptor right)
			throws StandardException
	{
		return SQLBoolean.truthValue(left,
									 right,
									 left.getShort() != right.getShort());
	}

	/**
	 * The < operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the <
	 * @param right			The value on the right side of the <
	 *						is not.
	 *
	 * @return	A SQL boolean value telling whether the first operand is less
	 *			than the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue lessThan(DataValueDescriptor left,
							DataValueDescriptor right)
			throws StandardException
	{
		return SQLBoolean.truthValue(left,
									 right,
									 left.getShort() < right.getShort());
	}

	/**
	 * The > operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the >
	 * @param right			The value on the right side of the >
	 *
	 * @return	A SQL boolean value telling whether the first operand is greater
	 *			than the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue greaterThan(DataValueDescriptor left,
							DataValueDescriptor right)
			throws StandardException
	{
		return SQLBoolean.truthValue(left,
									 right,
									 left.getShort() > right.getShort());
	}

	/**
	 * The <= operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the <=
	 * @param right			The value on the right side of the <=
	 *
	 * @return	A SQL boolean value telling whether the first operand is less
	 *			than or equal to the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue lessOrEquals(DataValueDescriptor left,
							DataValueDescriptor right)
			throws StandardException
	{
		return SQLBoolean.truthValue(left,
									 right,
									 left.getShort() <= right.getShort());
	}

	/**
	 * The >= operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the >=
	 * @param right			The value on the right side of the >=
	 *
	 * @return	A SQL boolean value telling whether the first operand is greater
	 *			than or equal to the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue greaterOrEquals(DataValueDescriptor left,
							DataValueDescriptor right)
			throws StandardException
	{
		return SQLBoolean.truthValue(left,
									 right,
									 left.getShort() >= right.getShort());
	}




	/**
	 * This method implements the * operator for "smallint * smallint".
	 *
	 * @param left	The first value to be multiplied
	 * @param right	The second value to be multiplied
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLSmallint containing the result of the multiplication
	 *
	 * @exception StandardException		Thrown on error
	 */

	public NumberDataValue times(NumberDataValue left,
							NumberDataValue right,
							NumberDataValue result)
				throws StandardException
	{
		if (result == null)
		{
			result = getNullDVD(left, right);
		}

		if (left.isNull() || right.isNull())
		{
			result.setToNull();
			return result;
		}

		/*
		** Java does not check for overflow with integral types. We have to
		** check the result ourselves.
		**
			The setValue(int) will perform the overflow check.
		*/
		int product = left.getShort() * right.getShort();
		result.setValue(product);
		return result;
	}


	/**
		mod(smallint, smallint)
	*/
	public NumberDataValue mod(NumberDataValue dividend,
							 NumberDataValue divisor,
							 NumberDataValue result)
				throws StandardException
	{
		if (result == null)
		{
			result = new SQLSmallint();
		}

		if (dividend.isNull() || divisor.isNull())
		{
			result.setToNull();
			return result;
		}

		/* Catch divide by 0 */
		short shortDivisor = divisor.getShort();
		if (shortDivisor == 0)
		{
			throw StandardException.newException(SQLState.LANG_DIVIDE_BY_ZERO);
		}

		result.setValue(dividend.getShort() % shortDivisor);
		return result;
	}
	/**
	 * This method implements the unary minus operator for smallint.
	 *
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLSmalllint containing the result of the division
	 *
	 * @exception StandardException		Thrown on error
	 */

	public NumberDataValue minus(NumberDataValue result)
									throws StandardException
	{
		if (result == null)
		{
			result = new SQLSmallint();
		}

		if (this.isNull())
		{
			result.setToNull();
			return result;
		}

		int operandValue = this.getShort();

		result.setValue(-operandValue);
		return result;
	}

    /**
     * This method implements the isNegative method.
     *
     * @return  A boolean.  If this.value is negative, return true.
     *          For positive values or null, return false.
     */

    protected boolean isNegative()
    {
        return !isNull() && value < 0;
    }

	/*
	 * String display of value
	 */

	public String toString()
	{
		if (isNull())
			return "NULL";
		else
			return Short.toString(value);
	}

	/*
	 * Hash code
	 */
	public int hashCode()
	{
		long longVal = (long) value;
		return (int) (longVal ^ (longVal >> 32));
	}

	/*
	 * useful constants...
	 */
	static final int SMALLINT_LENGTH = 2;

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( SQLSmallint.class);

    public int estimateMemoryUsage()
    {
        return BASE_MEMORY_USAGE;
    }

	/*
	 * object state
	 */
	private short value;
	public Format getFormat() {
		return Format.SMALLINT;
	}

	public BigDecimal getBigDecimal() {
		return isNull() ? null : BigDecimal.valueOf(value);
	}

	@Override
	public void read(Row row, int ordinal) throws StandardException {
		if (row.isNullAt(ordinal))
			setToNull();
		else {
			isNull = false;
			value = row.getShort(ordinal);
		}
	}

	@Override
	public StructField getStructField(String columnName) {
		return DataTypes.createStructField(columnName, DataTypes.ShortType, true);
	}

	@Override
	public Object getSparkObject() throws StandardException {
		return getShort();
	}

	public void updateThetaSketch(UpdateSketch updateSketch) {
		updateSketch.update(value);
	}

	@Override
	public void setSparkObject(Object sparkObject) throws StandardException {
		if (sparkObject == null)
			setToNull();
		else {
			value = (Short) sparkObject; // Autobox, must be something better.
			setIsNull(false);
		}
	}

}
