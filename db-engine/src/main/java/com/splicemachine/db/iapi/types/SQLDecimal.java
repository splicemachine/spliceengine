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
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.cache.ClassSize;
import com.splicemachine.db.iapi.services.io.ArrayInputStream;
import com.splicemachine.db.iapi.services.io.Storable;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl.Format;
import com.yahoo.sketches.theta.UpdateSketch;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * SQLDecimal satisfies the DataValueDescriptor
 * interfaces (i.e., OrderableDataType). It implements a numeric/decimal column,
 * e.g. for * storing a column value; it can be specified
 * when constructed to not allow nulls. Nullability cannot be changed
 * after construction, as it affects the storage size and mechanism.
 * <p>
 * Because OrderableDataType is a subtype of DataType,
 * SQLDecimal can play a role in either a DataType/Row
 * or a OrderableDataType/Row, interchangeably.
 * <p>
 * We assume the store has a flag for nullness of the value,
 * and simply return a 0-length array for the stored form
 * when the value is null.
 *
 */
public final class SQLDecimal extends NumberDataType implements VariableSizeDataValue
{
	/**
	 * object state.  Note that scale and precision are
	 * always determined dynamically from value when
	 * it is not null.

       The field value can be null without the data value being null.
	   In this case the value is stored in rawData and rawScale. This
	   is to allow the minimal amount of work to read a SQLDecimal from disk.
	   Creating the BigDecimal is expensive as it requires allocating
	   three objects, the last two are a waste in the case the row does
	   not qualify or the row will be written out by the sorter before being
	   returned to the application.
		<P>
		This means that this field must be accessed for read indirectly through
		the getBigDecimal() method, and when setting it the rawData field must
		be set to null.

	 */
	private BigDecimal	value;

	public int precision = -1;

	public int scale = -1;

	/**
		See comments for value
	*/
	private byte[]		rawData;

	/**
		See comments for value
	*/
	private int			rawScale;

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( SQLDecimal.class);
    private static final int BIG_DECIMAL_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( BigDecimal.class);

    public int estimateMemoryUsage()
    {
        int sz = BASE_MEMORY_USAGE;
        if( null != value)
            sz += BIG_DECIMAL_MEMORY_USAGE + (value.unscaledValue().bitLength() + 8)/8;
        if( null != rawData)
            sz += rawData.length;
        return sz;
    }


	////////////////////////////////////////////////////////////////////
	//
	// CLASS INTERFACE
	//
	////////////////////////////////////////////////////////////////////
	/** no-arg constructor, required by Formattable */
	public SQLDecimal() {

	}

	public SQLDecimal(BigDecimal val)
	{
		setValue(val);
	}

	public SQLDecimal(BigDecimal val, int precision, int scale)
			throws StandardException {

		setValue(val);
		if ((value != null) && (scale >= 0)) {
			setValue(value.setScale(scale, BigDecimal.ROUND_HALF_UP));
		}
		if (value ==null) {
			this.precision = precision;
			this.scale = scale;
		}

	}

	public SQLDecimal(String val)
	{
		setValue(new BigDecimal(val));
	}

	/*
	 * DataValueDescriptor interface
	 * (mostly implemented in DataType)
	 *
	 */


	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public int	getInt() throws StandardException
	{
		if (isNull())
			return 0;

		try {
			long lv = getLong();

			if ((lv >= Integer.MIN_VALUE) && (lv <= Integer.MAX_VALUE))
				return (int) lv;

		} catch (StandardException ignored) {
		}

		throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "INTEGER");
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public byte	getByte() throws StandardException
	{
		if (isNull())
			return (byte)0;

		try {
			long lv = getLong();

			if ((lv >= Byte.MIN_VALUE) && (lv <= Byte.MAX_VALUE))
				return (byte) lv;

		} catch (StandardException ignored) {
		}

		throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "TINYINT");
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public short	getShort() throws StandardException
	{
		if (isNull())
			return (short)0;

		try {
			long lv = getLong();

			if ((lv >= Short.MIN_VALUE) && (lv <= Short.MAX_VALUE))
				return (short) lv;

		} catch (StandardException ignored) {
		}

		throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "SMALLINT");
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public long	getLong() throws StandardException
	{
		BigDecimal localValue = getBigDecimal();
		if (localValue == null)
			return (long)0;

		// Valid range for long is
		//   greater than Long.MIN_VALUE - 1
		// *and*
		//   less than Long.MAX_VALUE + 1
		//
		// This ensures that DECIMAL values with an integral value
		// equal to the Long.MIN/MAX_VALUE round correctly to those values.
		// e.g. 9223372036854775807.1  converts to 9223372036854775807
		// this matches DB2 UDB behaviour

		if (   (localValue.compareTo(MINLONG_MINUS_ONE) == 1)
			&& (localValue.compareTo(MAXLONG_PLUS_ONE) == -1)) {

			return localValue.longValue();
		}

		throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "BIGINT");
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public float getFloat() throws StandardException
	{
		BigDecimal localValue = getBigDecimal();
		if (localValue == null)
			return (float)0;

		// If the BigDecimal is out of range for the float
		// then positive or negative infinity is returned.

		return NumberDataType.normalizeREAL(localValue.floatValue());
	}

	/**
	 *
	 * If we have a value that is greater than the maximum double,
	 * exception is thrown.  Otherwise, ok.  If the value is less
	 * than can be represented by a double, ti will get set to
	 * the smallest double value.
	 *
	 * @exception StandardException thrown on failure to convert
	 */
	public double getDouble() throws StandardException
	{
		BigDecimal localValue = getBigDecimal();
		if (localValue == null)
			return (double)0;

		// If the BigDecimal is out of range for double
		// then positive or negative infinity is returned.
		return NumberDataType.normalizeDOUBLE(localValue.doubleValue());
	}

	public BigDecimal getBigDecimal()
	{
		if (isNull())
			return null;
		if ((value == null) && (rawData != null))
		{
			setValue(new BigDecimal(new BigInteger(rawData), rawScale));
		}

		return value;
	}

	/**
	 * DECIMAL implementation. Convert to a BigDecimal using getObject
	 * which will return a BigDecimal
	 */
	public int typeToBigDecimal() {
		return java.sql.Types.DECIMAL;
	}

    // 0 or null is false, all else is true
	public boolean	getBoolean() {
        BigDecimal localValue = getBigDecimal();
        return localValue != null && localValue.compareTo(ZERO) != 0;
    }

	public String	getString() {
		BigDecimal localValue = getBigDecimal();
		if (localValue == null)
			return null;
		else
			return localValue.toPlainString();
	}

	public Object getSparkObject() {
		return getObject();
	}

	@Override
	public Object getObject() {
		return getBigDecimal();
	}

	/* This is constructor used fo PyStoredProcedureResultSetFactory */
	public void setValue(Double theValue) throws StandardException{
		if(theValue==null){
			restoreToNull();
			return;
		}
		setValue(theValue.doubleValue());
	}

	/**
	 * Set the value from a correctly typed BigDecimal object.
	 * @throws StandardException
	 */
	void setObject(Object theValue) throws StandardException {
		setValue((BigDecimal) theValue);
	}

	protected void setFrom(DataValueDescriptor theValue) throws StandardException {
		setCoreValue(SQLDecimal.getBigDecimal(theValue));
	}

	public int	getLength()
	{
		return getDecimalValuePrecision();
	}

	// this is for DataType's error generator
	public String getTypeName() {
		return TypeId.DECIMAL_NAME;
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */

	/**
		Return my format identifier.

		@see com.splicemachine.db.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId()
	{
		return StoredFormatIds.SQL_DECIMAL_ID;
	}

	/*
	 * see if the decimal value is null.
	 */
	/* @see Storable#isNull
	 */
	private boolean evaluateNull()
	{
		return (value == null) && (rawData == null);
	}

	/**
	 * Distill the BigDecimal to a byte array and
	 * write out: <UL>
	 *	<LI> scale (zero or positive) as a byte </LI>
	 *	<LI> length of byte array as a byte</LI>
	 *	<LI> the byte array </LI> </UL>
	 *
	 */
	public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(isNull);
        if (isNull)
            return;

		int scale;
		byte[] byteArray;
		if (value != null) {
			scale = value.scale();

			// J2SE 5.0 introduced negative scale value for BigDecimals.
			// In previouse Java releases a negative scale was not allowed
			// (threw an exception on setScale and the constructor that took
			// a scale).
			//
			// Thus the Derby format for DECIMAL implictly assumed a
			// positive or zero scale value, and thus now must explicitly
			// be positive. This is to allow databases created under J2SE 5.0
			// to continue to be supported under JDK 1.3/JDK 1.4, ie. to continue
			// the platform independence, independent of OS/cpu and JVM.
			//
			// If the scale is negative set the scale to be zero, this results
			// in an unchanged value with a new scale. A BigDecimal with a
			// negative scale by definition is a whole number.
			// e.g. 1000 can be represented by:
			//    a BigDecimal with scale -3 (unscaled value of 1)
			// or a BigDecimal with scale 0 (unscaled value of 1000)

			if (scale < 0) {
				scale = 0;
				setValue(value.setScale(0));
			}

			BigInteger bi = value.unscaledValue();
			byteArray = bi.toByteArray();
		} else {
			scale = rawScale;
			byteArray = rawData;
		}

		if (SanityManager.DEBUG)
		{
			if (scale < 0)
				SanityManager.THROWASSERT("DECIMAL scale at writeExternal is negative "
					+ scale + " value " + toString());
		}

		out.writeByte(scale);
		out.writeByte(byteArray.length);
		out.write(byteArray);
	}

	/**
	 * Note the use of rawData: we reuse the array if the
	 * incoming array is the same length or smaller than
	 * the array length.
	 *
	 * @see java.io.Externalizable#readExternal
	 */
	public void readExternal(ObjectInput in) throws IOException {

        if (in.readBoolean()) {
            setCoreValue(null);
            return;
        }

		// clear the previous value to ensure that the
		// rawData value will be used
		value = null;

		rawScale = in.readUnsignedByte();
		int size = in.readUnsignedByte();

		/*
		** Allocate a new array if the data to read
		** is larger than the existing array, or if
		** we don't have an array yet.

        Need to use readFully below and NOT just read because read does not
        guarantee getting size bytes back, whereas readFully does (unless EOF).
        */
		if ((rawData == null) || size != rawData.length)
		{
			rawData = new byte[size];
		}
		in.readFully(rawData);
		isNull = evaluateNull();

	}
	public void readExternalFromArray(ArrayInputStream in) throws IOException
	{
		// clear the previous value to ensure that the
		// rawData value will be used
		value = null;

		rawScale = in.readUnsignedByte();
		int size = in.readUnsignedByte();

		/*
		** Allocate a new array if the data to read
		** is larger than the existing array, or if
		** we don't have an array yet.

        Need to use readFully below and NOT just read because read does not
        guarantee getting size bytes back, whereas readFully does (unless EOF).
        */
		if ((rawData == null) || size != rawData.length)
		{
			rawData = new byte[size];
		}
		in.readFully(rawData);
		isNull = evaluateNull();
	}

	/**
	 * @see Storable#restoreToNull
	 *
	 */
	public void restoreToNull()
	{
		value = null;
		rawData = null;
		isNull = true;
	}


	/** @exception StandardException		Thrown on error */
	protected int typeCompare(DataValueDescriptor arg) throws StandardException
	{
		BigDecimal otherValue = SQLDecimal.getBigDecimal(arg);
		return getBigDecimal().compareTo(otherValue);
	}

	/*
	 * DataValueDescriptor interface
	 */

    /**
     * @see DataValueDescriptor#cloneValue
     */
    public DataValueDescriptor cloneValue(boolean forceMaterialization)
	{
		try {
		    SQLDecimal decimal = new SQLDecimal(getBigDecimal(), precision, scale);
		    if (decimal.getPrecision() != precision)
		        decimal.setPrecision(precision);
		    if (decimal.getScale() != scale)
		        decimal.setScale(scale);
		    return decimal;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull() {
		try {
			return new SQLDecimal(null,precision,scale);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
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
			rawData = null;
			setValue(resultSet.getBigDecimal(colNumber));
	}
	/**
		Set the value into a PreparedStatement.

		@exception SQLException Error setting value in PreparedStatement
	*/
	public final void setInto(PreparedStatement ps, int position) throws SQLException {

		if (isNull()) {
			ps.setNull(position, java.sql.Types.DECIMAL);
			return;
		}

		ps.setBigDecimal(position, getBigDecimal());
	}

	/**
	 *
	 * <B> WARNING </B> there is no checking to make sure
	 * that theValue doesn't exceed the precision/scale of
	 * the current SQLDecimal.  It is just assumed that the
	 * SQLDecimal is supposed to take the precision/scale of
	 * the BigDecimalized String.
	 *
	 * @exception StandardException throws NumberFormatException
	 *		when the String format is not recognized.
	 */
	public void setValue(String theValue) throws StandardException
	{
		rawData = null;

		if (theValue == null)
		{
			restoreToNull();
		}
		else
		{
		    try
			{
				theValue = theValue.trim();
				rawData = null;
		        setValue(new BigDecimal(theValue));
			} catch (NumberFormatException nfe)
			{
			    throw invalidFormat();
			}
		}
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setValue(double theValue) throws StandardException
	{
		setCoreValue(NumberDataType.normalizeDOUBLE(theValue));
		isNull = evaluateNull();
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 */
	public void setValue(float theValue)
		throws StandardException
	{
		setCoreValue((double)NumberDataType.normalizeREAL(theValue));
		isNull = evaluateNull();
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 */
	public void setValue(long theValue)
	{
		value = BigDecimal.valueOf(theValue);
		rawData = null;
		isNull = evaluateNull();
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 */
	public void setValue(int theValue)
	{
		setValue((long)theValue);
		isNull = evaluateNull();
	}

	public void setValue(BigDecimal theValue)
	{
		setCoreValue(theValue);
	}

	/**
		Only to be called when the application sets a value using BigDecimal
		through setBigDecimal calls.
	*/
	public void setBigDecimal(Number theValue) throws StandardException
	{
		setCoreValue((BigDecimal) theValue);
	}

	/**
		Called when setting a DECIMAL value internally or from
		through a procedure or function.
		Handles long in addition to BigDecimal to handle
		identity being stored as a long but returned as a DECIMAL.
	*/
	public void setValue(Number theValue) throws StandardException
	{
		if (SanityManager.ASSERT)
		{
			if (theValue != null &&
				!(theValue instanceof java.math.BigDecimal) &&
				!(theValue instanceof java.lang.Long))
				SanityManager.THROWASSERT("SQLDecimal.setValue(Number) passed a " + theValue.getClass());
		}

		if (theValue instanceof BigDecimal || theValue == null)
			setCoreValue((BigDecimal) theValue);
		else
			setValue(theValue.longValue());
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 */
	public void setValue(boolean theValue)
	{
		setCoreValue(theValue ? ONE : ZERO);
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.DECIMAL_PRECEDENCE;
	}
    // END DataValueDescriptor interface

	private void setCoreValue(BigDecimal theValue)
	{
		value = theValue;
		if (value !=null) {
			precision = getValuePrecision();
			scale = value.scale();
		}
		rawData = null;
		isNull = evaluateNull();
	}

	private void setCoreValue(double theValue) {
		value = new BigDecimal(Double.toString(theValue));
		if (value !=null) {
			precision = getValuePrecision();
			scale = value.scale();
		}
		rawData = null;
		isNull = evaluateNull();
	}

	/**
	 * Normalization method - this method may be called when putting
	 * a value into a SQLDecimal, for example, when inserting into a SQLDecimal
	 * column.  See NormalizeResultSet in execution.
	 * <p>
	 * Note that truncation is allowed on the decimal portion
	 * of a numeric only.
	 *
	 * @param desiredType	The type to normalize the source column to
	 * @param source		The value to normalize
	 *
	 * @throws StandardException				Thrown for null into
	 *											non-nullable column, and for
	 *											truncation error
	 */
	public void normalize(
				DataTypeDescriptor desiredType,
				DataValueDescriptor source)
						throws StandardException
	{
		int desiredScale = desiredType.getScale();
		int desiredPrecision = desiredType.getPrecision();

		setFrom(source);
		setWidth(desiredPrecision, desiredScale, true);
	}


	/*
	** SQL Operators
	*/


	/**
	 * This method implements the + operator for DECIMAL.
	 *
	 * @param addend1	One of the addends
	 * @param addend2	The other addend
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLDecimal containing the result of the addition
	 *
	 * @exception StandardException		Thrown on error
	 */

	public NumberDataValue plus(NumberDataValue addend1,
							NumberDataValue addend2,
							NumberDataValue result)
				throws StandardException
	{
		if (result == null)
		{
			result = new SQLDecimal();
		}

		if (addend1.isNull() || addend2.isNull())
		{
			result.setToNull();
			return result;
		}

		result.setBigDecimal(SQLDecimal.getBigDecimal(addend1).add(SQLDecimal.getBigDecimal(addend2)));
		return result;
	}

	/**
	 * This method implements the - operator for "decimal - decimal".
	 *
	 * @param left	The value to be subtracted from
	 * @param right	The value to be subtracted
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLDecimal containing the result of the subtraction
	 *
	 * @exception StandardException		Thrown on error
	 */

	public NumberDataValue minus(NumberDataValue left,
							NumberDataValue right,
							NumberDataValue result)
				throws StandardException
	{
		if (result == null)
		{
			result = new SQLDecimal();
		}

		if (left.isNull() || right.isNull())
		{
			result.setToNull();
			return result;
		}

		result.setBigDecimal(SQLDecimal.getBigDecimal(left).subtract(SQLDecimal.getBigDecimal(right)));
		return result;
	}

	/**
	 * This method implements the * operator for "double * double".
	 *
	 * @param left	The first value to be multiplied
	 * @param right	The second value to be multiplied
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLDecimal containing the result of the multiplication
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
			result = new SQLDecimal();
		}

		if (left.isNull() || right.isNull())
		{
			result.setToNull();
			return result;
		}

		result.setBigDecimal(SQLDecimal.getBigDecimal(left).multiply(SQLDecimal.getBigDecimal(right)));
		return result;
	}

	/**
	 * This method implements the / operator for BigDecimal/BigDecimal
	 *
	 * @param dividend	The numerator
	 * @param divisor	The denominator
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLDecimal containing the result of the division
	 *
	 * @exception StandardException		Thrown on error
	 */

	public NumberDataValue divide(NumberDataValue dividend,
							 NumberDataValue divisor,
							 NumberDataValue result)
				throws StandardException
	{
		return divide(dividend, divisor, result, -1);
	}

	/**
	 * This method implements the / operator for BigDecimal/BigDecimal
	 *
	 * @param dividend	The numerator
	 * @param divisor	The denominator
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 * @param scale		The result scale, if < 0, calculate the scale according
	 *					to the actual values' sizes
	 *
	 * @return	A SQLDecimal containing the result of the division
	 *
	 * @exception StandardException		Thrown on error
	 */

	public NumberDataValue divide(NumberDataValue dividend,
							 NumberDataValue divisor,
							 NumberDataValue result,
							 int scale)
				throws StandardException
	{
		if (result == null)
		{
			result = new SQLDecimal();
		}

		if (dividend.isNull() || divisor.isNull())
		{
			result.setToNull();
			return result;
		}

		BigDecimal divisorBigDecimal = SQLDecimal.getBigDecimal(divisor);

		if (divisorBigDecimal.compareTo(ZERO) == 0)
		{
			throw  StandardException.newException(SQLState.LANG_DIVIDE_BY_ZERO);
		}
		BigDecimal dividendBigDecimal = SQLDecimal.getBigDecimal(dividend);

		/*
		** Set the result scale to be either the passed in scale, whcih was
		** calculated at bind time to be max(ls+rp-rs+1, 4), where ls,rp,rs
		** are static data types' sizes, which are predictable and stable
		** (for the whole result set column, eg.); otherwise dynamically
		** calculates the scale according to actual values.  Beetle 3901
		*/
		result.setBigDecimal(dividendBigDecimal.divide(
									divisorBigDecimal,
									scale > -1 ? scale :
									Math.max((dividendBigDecimal.scale() +
											SQLDecimal.getWholeDigits(divisorBigDecimal) +
											1),
										NumberDataValue.MIN_DECIMAL_DIVIDE_SCALE),
									BigDecimal.ROUND_HALF_UP));

		return result;
	}

	/**
	 * This method implements the unary minus operator for double.
	 *
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLDecimal containing the result of the division
	 *
	 * @exception StandardException		Thrown on error
	 */

	public NumberDataValue minus(NumberDataValue result)
									throws StandardException
	{
		if (result == null)
		{
			result = new SQLDecimal();
		}

		if (this.isNull())
		{
			result.setToNull();
			return result;
		}

		result.setBigDecimal(getBigDecimal().negate());
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
        return !isNull() && (getBigDecimal().compareTo(ZERO) == -1);
    }

	/*
	 * String display of value
	 */
	public String toString()
	{
		if (isNull())
			return "NULL";
		else
			return getString();
	}

	/*
	 * Hash code
	 */
	public int hashCode()
	{
		long longVal;
		BigDecimal localValue = getBigDecimal();

		double doubleVal = (localValue != null) ? localValue.doubleValue() : 0;

		if (Double.isInfinite(doubleVal))
		{
			/*
			 ** This loses the fractional part, but it probably doesn't
			 ** matter for numbers that are big enough to overflow a double -
			 ** it's probably rare for numbers this big to be different only in
			 ** their fractional parts.
			 */
			longVal = localValue.longValue();
		}
		else
		{
			longVal = (long) doubleVal;
			if (longVal != doubleVal)
			{
				longVal = Double.doubleToLongBits(doubleVal);
			}
		}

		return (int) (longVal ^ (longVal >> 32));
	}

	///////////////////////////////////////////////////////////////////////////////
	//
	// VariableSizeDataValue interface
	//
	///////////////////////////////////////////////////////////////////////////////

	/**
	 * Set the precision/scale of the to the desired values.
	 * Used when CASTing.  Ideally we'd recycle normalize(), but
	 * the use is different.
	 *
	 * @param desiredPrecision	the desired precision -- IGNORE_PREICISION
	 *					if it is to be ignored.
	 * @param desiredScale	the desired scale
	 * @param errorOnTrunc	throw error on truncation (ignored --
	 *		always thrown if we truncate the non-decimal part of
	 *		the value)
	 *
	 * @exception StandardException		Thrown on non-zero truncation
	 *		if errorOnTrunc is true
	 */
	public void setWidth(int desiredPrecision,
			int desiredScale,
			boolean errorOnTrunc)
			throws StandardException
	{
		if (isNull())
			return;

		if (desiredPrecision != IGNORE_PRECISION &&
			((desiredPrecision - desiredScale) <  SQLDecimal.getWholeDigits(getBigDecimal())))
		{
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE,
									("DECIMAL/NUMERIC("+desiredPrecision+","+desiredScale+")"));
		}
		rawData = null;
		setValue(value.setScale(desiredScale, BigDecimal.ROUND_HALF_UP));
	}

	/**
	 * Return the SQL scale of this value, number of digits after the
	 * decimal point, or zero for a whole number. This does not match the
	 * return from BigDecimal.scale() since in J2SE 5.0 onwards that can return
	 * negative scales.
	 */
	public int getDecimalValuePrecision()
	{
		if (isNull())
			return 0;

		BigDecimal localValue = getBigDecimal();

		return SQLDecimal.getWholeDigits(localValue) + getDecimalValueScale();
	}

	/**
	 * Return the SQL scale of this value, number of digits after the
	 * decimal point, or zero for a whole number. This does not match the
	 * return from BigDecimal.scale() since in J2SE 5.0 onwards that can return
	 * negative scales.
	 */
	public int getDecimalValueScale()
	{
		if (isNull())
			return 0;

		if (value == null)
			return rawScale;

		int scale = value.scale();
		if (scale >= 0)
			return scale;

		// BigDecimal scale is negative, so number must have no fractional
		// part as its value is the unscaled value * 10^-scale
		return 0;
	}

	/**
	 * Get a BigDecimal representing the value of a DataValueDescriptor
	 * @param value Non-null value to be converted
	 * @return BigDecimal value
	 * @throws StandardException Invalid conversion or out of range.
	 */
	public static BigDecimal getBigDecimal(DataValueDescriptor value) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			if (value.isNull())
				SanityManager.THROWASSERT("NULL value passed to SQLDecimal.getBigDecimal");
		}

		switch (value.typeToBigDecimal())
		{
		case Types.DECIMAL:
			return (BigDecimal) value.getObject();
		case Types.CHAR:
			try {
				return new BigDecimal(value.getString().trim());
			} catch (NumberFormatException nfe) {
				throw StandardException.newException(SQLState.LANG_FORMAT_EXCEPTION, "java.math.BigDecimal");
			}
		case Types.BIGINT:
			return BigDecimal.valueOf(value.getLong());
		default:
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("invalid return from " + value.getClass() + ".typeToBigDecimal() " + value.typeToBigDecimal());
			return null;
		}
	}

	/**
	 * Calculate the number of digits to the left of the decimal point
	 * of the passed in value.
	 * @param decimalValue Value to get whole digits from, never null.
	 * @return number of whole digits.
	 */
	private static int getWholeDigits(BigDecimal decimalValue) {
        /**
         * if ONE > abs(value) then the number of whole digits is 0
         */
        decimalValue = decimalValue.abs();
        if (ONE.compareTo(decimalValue) == 1)
            return 0;
        return decimalValue.precision() - decimalValue.scale();
	}

	public Format getFormat() {
		return Format.DECIMAL;
	}

	@Override
	public void read(Row row, int ordinal) throws StandardException {
		if (row.isNullAt(ordinal))
			setToNull();
		else {
			isNull = false;
			value = row.getDecimal(ordinal);
		}
	}

	/**
	 * Get Precision of SQLDecimal
	 *
	 * @return
     */
	public int getPrecision() {
		return precision;
	}

	/**
	 *
	 * Set Precision of SQLDecimal
	 *
	 * @param precision
     */
	public void setPrecision(int precision) {
		this.precision = precision;
	}

	/**
	 *
	 * Get scale of SQLDecimal
	 *
	 * @return
     */
	public int getScale() {
		return scale;
	}

	/**
	 *
	 * Set Scale of SQLDecimal
	 *
	 * @param scale
     */
	public void setScale(int scale) {
		this.scale = scale;
	}

	@Override
	public StructField getStructField(String columnName) {
		if (precision == -1 || scale == -1) {
			return DataTypes.createStructField(columnName, DecimalType.SYSTEM_DEFAULT(),true);
		} else {
			return DataTypes.createStructField(columnName, DataTypes.createDecimalType(precision, scale), true);
		}
	}

	public void updateThetaSketch(UpdateSketch updateSketch) {
		updateSketch.update(this.value.toEngineeringString());
	}

	@Override
	public void setSparkObject(Object sparkObject) throws StandardException {
		if (sparkObject == null)
			setToNull();
		else {
			value = (BigDecimal) sparkObject; //
			setIsNull(false);
		}
	}

	private int getValuePrecision() {
		if (value == null)
			return 0;
		// for decimal 0, it is possible that precision is less than scale
		// after the call of BigDecimal.setScale(), e.g., cast(0 as decimal(14,4)).
		// In that case, the BigDecimal 0 starts with precision 1 and scale 0,
		// and ends with precision 1 and scale 4, so adjust the precision accordingly
		if (value.signum() == 0)
			return value.scale() + 1;

		return value.precision();
	}
}
