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

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.ArrayInputStream;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.io.Storable;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.error.StandardException;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl.Format;
import com.yahoo.sketches.theta.UpdateSketch;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

/**
 * SQLDouble satisfies the DataValueDescriptor
 * interfaces (i.e., OrderableDataType). It implements a double column, 
 * e.g. for * storing a column value; it can be specified
 * when constructed to not allow nulls. Nullability cannot be changed
 * after construction, as it affects the storage size and mechanism.
 * <p>
 * Because OrderableDataType is a subtype of DataType,
 * SQLDouble can play a role in either a DataType/Row
 * or a OrderableDataType/Row, interchangeably.
 * <p>
 * We assume the store has a flag for nullness of the value,
 * and simply return a 0-length array for the stored form
 * when the value is null.
 * <p>
 * PERFORMANCE: There are likely alot of performance improvements
 * possible for this implementation -- it new's Double
 * more than it probably wants to.
 * <p>
 * This is modeled after SQLInteger.
 * <p>
 * We don't let doubles get constructed with NaN or Infinity values, and
 * check for those values where they can occur on operations, so the
 * set* operations do not check for them coming in.
 *
 */
public final class SQLDouble extends NumberDataType
{

	/*
	 * DataValueDescriptor interface
	 * (mostly implemented in DataType)
	 */


    // JDBC is lax in what it permits and what it
	// returns, so we are similarly lax
	// @see DataValueDescriptor
	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public int	getInt() throws StandardException
	{
	    // REMIND: do we want to check for truncation?
		if ((value > (((double) Integer.MAX_VALUE) + 1.0d)) || (value < (((double) Integer.MIN_VALUE) - 1.0d)))
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "INTEGER");
		return (int)value;
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public byte	getByte() throws StandardException
	{
		if ((value > (((double) Byte.MAX_VALUE) + 1.0d)) || (value < (((double) Byte.MIN_VALUE) - 1.0d)))
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "TINYINT");
		return (byte) value;
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public short	getShort() throws StandardException
	{
		if ((value > (((double) Short.MAX_VALUE) + 1.0d)) || (value < (((double) Short.MIN_VALUE) - 1.0d)))
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "SMALLINT");
		return (short) value;
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public long	getLong() throws StandardException
	{
		if ((value > (((double) Long.MAX_VALUE) + 1.0d)) || (value < (((double) Long.MIN_VALUE) - 1.0d)))
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "BIGINT");
		return (long) value;
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public float	getFloat() throws StandardException
	{
		if (Float.isInfinite((float)value))
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, TypeId.REAL_NAME);
		return (float) value;
	}

	public double	getDouble() throws StandardException {
        if (Double.isInfinite(value)) {
            throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, TypeId.DOUBLE_NAME);
        }
		/* This value is bogus if the SQLDouble is null */
		return value;
	}

	/**
	 * DOUBLE implementation. Convert to a BigDecimal using getString.
	 */
	public int typeToBigDecimal()
	{
		return java.sql.Types.CHAR;
	}
    // for lack of a specification: getDouble()==0 gives true
    // independent of the NULL flag
	public boolean	getBoolean()
	{
		return (value != 0);
	}

	public String	getString()
	{
		if (isNull())
			return null;
		else
			return Double.toString(value);
	}

	public Object	getObject()
	{
		// REMIND: could create one Double and reuse it?
		if (isNull())
			return null;
		else
			return new Double(value);
	}


	/**
	 * Set the value from a correctly typed Double object.
	 * @throws StandardException 
	 */
	void setObject(Object theValue) throws StandardException
	{
		setValue(((Double) theValue).doubleValue());
	}
	
	protected void setFrom(DataValueDescriptor theValue) throws StandardException {
		setValue(theValue.getDouble());
	}

	public int	getLength()
	{
		return DOUBLE_LENGTH;
	}

	// this is for DataType's error generator
	public String getTypeName()
	{
		return TypeId.DOUBLE_NAME;
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */

	/**
		Return my format identifier.

		@see com.splicemachine.db.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_DOUBLE_ID;
	}

	public void writeExternal(ObjectOutput out) throws IOException {


		out.writeBoolean(isNull);
		out.writeDouble(value);
	}

	/** @see java.io.Externalizable#readExternal */
	public void readExternal(ObjectInput in) throws IOException {
		isNull = in.readBoolean();
		value = in.readDouble();
	}

	/** @see java.io.Externalizable#readExternal */
	public void readExternalFromArray(ArrayInputStream in) throws IOException {
		isNull = in.readBoolean();
		value = in.readDouble();
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

		double thisValue = this.getDouble();

		double otherValue = arg.getDouble();

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
			return new SQLDouble(value, isNull, true); // Do not test me again on cloning : JL
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLDouble();
	}

	/** 
	 * @see DataValueDescriptor#setValueFromResultSet 
	 *
	 * @exception StandardException		Thrown on error
	 * @exception SQLException		Thrown on error
	 */
	public void setValueFromResultSet(ResultSet resultSet, int colNumber,
									  boolean isNullable)
		throws StandardException, SQLException
	{
			double dv = resultSet.getDouble(colNumber);
			isNull = (isNullable && resultSet.wasNull());
            if (isNull)
                value = 0;
            else 
                value = NumberDataType.normalizeDOUBLE(dv);
	}
	/**
		Set the value into a PreparedStatement.

		@exception SQLException Error setting value in PreparedStatement
	*/
	public final void setInto(PreparedStatement ps, int position) throws SQLException {

		if (isNull()) {
			ps.setNull(position, java.sql.Types.DOUBLE);
			return;
		}

		ps.setDouble(position, value);
	}
	/**
		Set this value into a ResultSet for a subsequent ResultSet.insertRow
		or ResultSet.updateRow. This method will only be called for non-null values.

		@exception SQLException thrown by the ResultSet object
		@exception StandardException thrown by me accessing my value.
	*/
	public final void setInto(ResultSet rs, int position) throws SQLException, StandardException {
		rs.updateDouble(position, value);
	}
	/*
	 * class interface
	 */

	/*
	 * constructors
	 */

	/** no-arg constructor, required by Formattable */
    // This constructor also gets used when we are
    // allocating space for a double.
	public SQLDouble() {
		isNull = true;
	}

	public SQLDouble(double val) throws StandardException
	{
		setValue(NumberDataType.normalizeDOUBLE(val));
	}

	public SQLDouble(Double obj) throws StandardException {
		if (isNull = (obj == null))
            ;
		else
			setValue(NumberDataType.normalizeDOUBLE(obj.doubleValue()));
	}

	private SQLDouble(double val, boolean startsnull) throws StandardException
	{
		value = NumberDataType.normalizeDOUBLE(val); // maybe only do if !startsnull
		isNull = startsnull;
	}
	/**
	 * Method to create double with normalization, normalize ignored
	 */
	private SQLDouble(double val, boolean startsnull, boolean normalize)
	{
		value = val; // maybe only do if !startsnull
		isNull = startsnull;
	}

	
	/**
		@exception StandardException throws NumberFormatException
			when the String format is not recognized.
	 */
	public void setValue(String theValue) throws StandardException
	{
		if (theValue == null)
		{
			value = 0;
			isNull = true;
		}
		else
		{
			double doubleValue = 0;
		    try {
                // ??? jsk: rounding???
		        doubleValue = Double.parseDouble(theValue.trim());
			} catch (NumberFormatException nfe) {
			    throw invalidFormat();
			}
			value = NumberDataType.normalizeDOUBLE(doubleValue);
			isNull = false;
		}
	}

	/**
	 * @exception StandardException on NaN or Infinite double
	 */
	public void setValue(double theValue) throws StandardException
	{
		value = NumberDataType.normalizeDOUBLE(theValue);
		isNull = false;
	}

	/**
	 * @exception StandardException on NaN or Infinite float
	 */
	public void setValue(float theValue) throws StandardException
	{
		value = NumberDataType.normalizeDOUBLE(theValue);
		isNull = false;
	}

	public void setValue(long theValue)
	{
		value = theValue; // no check needed
		isNull = false;
	}

	public void setValue(int theValue)
	{
		value = theValue; // no check needed
		isNull = false;
	}

	public  void setValue(Number theValue) throws StandardException
	{
		if (objectNull(theValue)) 
			return;

		if (SanityManager.ASSERT)
		{
			if (!(theValue instanceof java.lang.Double))
				SanityManager.THROWASSERT("SQLDouble.setValue(Number) passed a " + theValue.getClass());
		}

		setValue(theValue.doubleValue());
	}

	/**
		Called for an application setting this value using a BigDecimal 
	*/
	public  void setBigDecimal(Number bigDecimal) throws StandardException
	{
		if (objectNull(bigDecimal)) 
			return;

		// Note BigDecimal.doubleValue() handles the case where
		// its value is outside the range of a double. It returns
		// infinity values which should throw an exception in setValue(double).
		setValue(bigDecimal.doubleValue());
		
	}

	/**
	 * @see NumberDataValue#setValue
	 *
	 */
	public void setValue(boolean theValue)
	{
		value = theValue?1:0;
		isNull = false;
	}


	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.DOUBLE_PRECEDENCE;
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
	 *						is not.
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
									 left.getDouble() == right.getDouble());
	}

	/**
	 * The <> operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the <>
	 * @param right			The value on the right side of the <>
	 *						is not.
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
									 left.getDouble() != right.getDouble());
	}

	/**
	 * The < operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the <
	 * @param right			The value on the right side of the <
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
									 left.getDouble() < right.getDouble());
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
									 left.getDouble() > right.getDouble());
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
									 left.getDouble() <= right.getDouble());
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
									 left.getDouble() >= right.getDouble());
	}

	/**
	 * This method implements the + operator for "double + double".
	 *
	 * @param addend1	One of the addends
	 * @param addend2	The other addend
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLDouble containing the result of the addition
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
			result = new SQLDouble();
		}

		if (addend1.isNull() || addend2.isNull())
		{
			result.setToNull();
			return result;
		}

		double tmpresult = addend1.getDouble() + addend2.getDouble();
        // No need to check underflow (result rounded to 0.0),
        // since the difference between two non-equal valid DB2 DOUBLE values is always non-zero in java.lang.Double precision.
		result.setValue(tmpresult);

		return result;
	}

	/**
	 * This method implements the - operator for "double - double".
	 *
	 * @param left	The value to be subtracted from
	 * @param right	The value to be subtracted
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLDouble containing the result of the subtraction
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
			result = new SQLDouble();
		}

		if (left.isNull() || right.isNull())
		{
			result.setToNull();
			return result;
		}

		double tmpresult = left.getDouble() - right.getDouble();
        // No need to check underflow (result rounded to 0.0),
        // since no difference between two valid DB2 DOUBLE values can be rounded off to 0.0 in java.lang.Double
		result.setValue(tmpresult);
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
	 * @return	A SQLDouble containing the result of the multiplication
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
			result = new SQLDouble();
		}

		if (left.isNull() || right.isNull())
		{
			result.setToNull();
			return result;
		}

        double leftValue = left.getDouble();
        double rightValue = right.getDouble();
		double tempResult = leftValue * rightValue;
        // check underflow (result rounded to 0.0)
        if ( (tempResult == 0.0) && ( (leftValue != 0.0) && (rightValue != 0.0) ) ) {
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, TypeId.DOUBLE_NAME);
        }

		result.setValue(tempResult);
		return result;
	}

	/**
	 * This method implements the / operator for "double / double".
	 *
	 * @param dividend	The numerator
	 * @param divisor	The denominator
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLDouble containing the result of the division
	 *
	 * @exception StandardException		Thrown on error
	 */

	public NumberDataValue divide(NumberDataValue dividend,
							 NumberDataValue divisor,
							 NumberDataValue result)
				throws StandardException
	{
		if (result == null)
		{
			result = new SQLDouble();
		}

		if (dividend.isNull() || divisor.isNull())
		{
			result.setToNull();
			return result;
		}

		/*
		** For double division, we can't catch divide by zero with Double.NaN;
		** So we check the divisor before the division.
		*/

		double divisorValue = divisor.getDouble();

		if (divisorValue == 0.0e0D)
		{
			throw StandardException.newException(SQLState.LANG_DIVIDE_BY_ZERO);
		}

        double dividendValue = dividend.getDouble();
		double divideResult =  dividendValue / divisorValue;

		if (Double.isNaN(divideResult))
		{
			throw StandardException.newException(SQLState.LANG_DIVIDE_BY_ZERO);
		}

        // check underflow (result rounded to 0.0d)
        if ((divideResult == 0.0d) && (dividendValue != 0.0d)) {
			throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, TypeId.DOUBLE_NAME);
        }

		result.setValue(divideResult);
		return result;
	}

	/**
	 * This method implements the unary minus operator for double.
	 *
	 * @param result	The result of a previous call to this method, null
	 *					if not called yet
	 *
	 * @return	A SQLDouble containing the result of the division
	 *
	 * @exception StandardException		Thrown on error
	 */

	public NumberDataValue minus(NumberDataValue result)
									throws StandardException
	{
		double		minusResult;

		if (result == null)
		{
			result = new SQLDouble();
		}

		if (this.isNull())
		{
			result.setToNull();
			return result;
		}

		/*
		** Doubles are assumed to be symmetric -- that is, their
		** smallest negative value is representable as a positive
		** value, and vice-versa.
		*/
		minusResult = -(this.getDouble());

		result.setValue(minusResult);
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
        return !isNull() && (value < 0.0d);
    }

	/*
	 * String display of value
	 */

	public String toString()
	{
		if (isNull())
			return "NULL";
		else
			return Double.toString(value);
	}

	/*
	 * Hash code
	 */
	public int hashCode()
	{
		long longVal = (long) value;
		double doubleLongVal = (double) longVal;

		/*
		** NOTE: This is coded to work around a bug in Visual Cafe 3.0.
		** If longVal is compared directly to value on that platform
		** with the JIT enabled, the values will not always compare
		** as equal even when they should be equal. This happens with
		** the value Long.MAX_VALUE, for example.
		**
		** Assigning the long value back to a double and then doing
		** the comparison works around the bug.
		**
		** This fixes Cloudscape bug number 1757.
		**
		**		-	Jeff Lichtman
		*/
		if (doubleLongVal != value)
        {
			longVal = Double.doubleToLongBits(value);
		}

		return (int) (longVal ^ (longVal >> 32));	
	}

	/*
	 * useful constants...
	 */
	static final int DOUBLE_LENGTH		= 32; // must match the number of bytes written by DataOutput.writeDouble()

    private static final int BASE_MEMORY_USAGE = 8;

    public int estimateMemoryUsage()
    {
        return BASE_MEMORY_USAGE;
    }

	/*
	 * object state
	 */
	private double	value;

    public boolean isDoubleType() {
        return true;
    }
    
    public Format getFormat() {
    	return Format.DOUBLE;
    }

	public BigDecimal getBigDecimal() {
		return isNull() ? null : BigDecimal.valueOf(value);
	}

	/**
	 *
	 * Write into Project Tungsten Format (UnsafeRow)
	 *
	 * @see UnsafeRowWriter#write(int, double)
	 *
	 * @param unsafeRowWriter
	 * @param ordinal
     */
	@Override
	public void write(UnsafeRowWriter unsafeRowWriter, int ordinal) {
		if (isNull())
				unsafeRowWriter.setNullAt(ordinal);
		else
			unsafeRowWriter.write(ordinal,value);
	}

	/**
	 *
	 * Read from a Project Tungsten Format.
	 *
	 * @see UnsafeRow#getDouble(int)
	 *
	 * @param unsafeRow
	 * @param ordinal
	 * @throws StandardException
     */
	@Override
	public void read(UnsafeRow unsafeRow, int ordinal) throws StandardException {
		if (unsafeRow.isNullAt(ordinal))
				setToNull();
		else {
			isNull = false;
			value = unsafeRow.getDouble(ordinal);
		}
	}

	@Override
	public void read(Row row, int ordinal) throws StandardException {
		if (row.isNullAt(ordinal))
			setToNull();
		else {
			isNull = false;
			value = row.getDouble(ordinal);
		}
	}


	/**
	 *
	 * Get Encoded Key Length.
	 *
	 * @return
	 * @throws StandardException
     */
	@Override
	public int encodedKeyLength() throws StandardException {
		return isNull()?1:9;
	}

	/**
	 *
	 * Encode into Key.
	 *
	 * @see OrderedBytes#encodeFloat64(PositionedByteRange, double, Order)
	 *
	 * @param src
	 * @param order
	 * @throws StandardException
     */
	@Override
	public void encodeIntoKey(PositionedByteRange src, Order order) throws StandardException {
		if (isNull())
				OrderedBytes.encodeNull(src, order);
		else
			OrderedBytes.encodeFloat64(src, value, order);
	}

	/**
	 *
	 * Decode from Key.
	 *
	 * @see OrderedBytes#decodeFloat64(PositionedByteRange)
	 *
 	 * @param src
	 * @throws StandardException
     */
	@Override
	public void decodeFromKey(PositionedByteRange src) throws StandardException {
		if (OrderedBytes.isNull(src))
				setToNull();
		else
			value = OrderedBytes.decodeFloat64(src);
	}

	@Override
	public StructField getStructField(String columnName) {
		return DataTypes.createStructField(columnName, DataTypes.DoubleType, true);
	}


	public void updateThetaSketch(UpdateSketch updateSketch) {
		updateSketch.update(value);
	}
}
