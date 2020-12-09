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

import com.google.protobuf.ByteString;
import com.splicemachine.db.catalog.types.CatalogMessage;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.cache.ClassSize;
import com.splicemachine.db.iapi.services.io.*;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl.Format;
import com.yahoo.sketches.theta.UpdateSketch;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Objects;

/**
 * SQLDecfloat satisfies the DataValueDescriptor
 * interfaces (i.e., OrderableDataType). It implements a numeric/decimal column,
 * e.g. for * storing a column value; it can be specified
 * when constructed to not allow nulls. Nullability cannot be changed
 * after construction, as it affects the storage size and mechanism.
 * <p>
 * Because OrderableDataType is a subtype of DataType,
 * SQLDecfloat can play a role in either a DataType/Row
 * or a OrderableDataType/Row, interchangeably.
 * <p>
 * We assume the store has a flag for nullness of the value,
 * and simply return a 0-length array for the stored form
 * when the value is null.
 *
 */
public final class SQLDecfloat extends NumberDataType
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
    private BigDecimal value;

    /**
        See comments for value
    */
    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( SQLDecfloat.class);
    private static final int BIG_DECIMAL_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( BigDecimal.class);

    public int estimateMemoryUsage()
    {
        int sz = BASE_MEMORY_USAGE;
        if( null != value)
            sz += BIG_DECIMAL_MEMORY_USAGE;
        return sz;
    }


    ////////////////////////////////////////////////////////////////////
    //
    // CLASS INTERFACE
    //
    ////////////////////////////////////////////////////////////////////
    /** no-arg constructor, required by Formattable */
    public SQLDecfloat() {

    }

    public SQLDecfloat(BigDecimal val)
    {
        setValue(val);
    }

    public SQLDecfloat(String val)
    {
        setValue(new BigDecimal(val, MathContext.DECIMAL128));
    }

    public SQLDecfloat(CatalogMessage.SQLDecfloat sqlDecfloat) {
        init(sqlDecfloat);
    }

    /*
     * DataValueDescriptor interface
     * (mostly implemented in DataType)
     *
     */


                       /**
     * @exception StandardException thrown on failure to convert
     */
    public int    getInt() throws StandardException
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
    public byte    getByte() throws StandardException
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
    public short    getShort() throws StandardException
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
    public long    getLong() throws StandardException
    {
        BigDecimal localValue = getBigDecimal();
        if (localValue == null)
            return 0;

        // Valid range for long is
        //   greater than Long.MIN_VALUE - 1
        // *and*
        //   less than Long.MAX_VALUE + 1
        //
        // This ensures that DECIMAL values with an integral value
        // equal to the Long.MIN/MAX_VALUE round correctly to those values.
        // e.g. 9223372036854775807.1  converts to 9223372036854775807
        // this matches DB2 UDB behaviour

        if (   (localValue.compareTo(MINLONG_MINUS_ONE) > 0)
            && (localValue.compareTo(MAXLONG_PLUS_ONE) < 0)) {

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
            return 0;

        // If the BigDecimal is out of range for double
        // then positive or negative infinity is returned.
        return NumberDataType.normalizeDOUBLE(localValue.doubleValue());
    }

    public BigDecimal getBigDecimal()
    {
        if (isNull())
            return null;

        return value;
    }

    /**
     * DECIMAL implementation. Convert to a BigDecimal using getObject
     * which will return a BigDecimal
     */
    public int typeToBigDecimal() {
        return com.splicemachine.db.iapi.reference.Types.DECFLOAT;
    }

    // 0 or null is false, all else is true
    public boolean    getBoolean() {
        BigDecimal localValue = getBigDecimal();
        return localValue != null && localValue.compareTo(ZERO) != 0;
    }

    public String    getString() {
        BigDecimal localValue = getBigDecimal();
        if (localValue == null)
            return null;
        else
            return localValue.toPlainString();
    }

    public Object getSparkObject() {
        return getString();
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
        setCoreValue(SQLDecfloat.getBigDecimal(theValue));
    }

    public int    getLength()
    {
        return getDecimalValuePrecision();
    }

    // this is for DataType's error generator
    public String getTypeName() {
        return TypeId.DECFLOAT_NAME;
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
        return StoredFormatIds.SQL_DECFLOAT_ID;
    }

    /*
     * see if the decimal value is null.
     */
    /* @see Storable#isNull
     */
    private boolean evaluateNull()
    {
        return (value == null);
    }

    @Override
    public CatalogMessage.DataValueDescriptor toProtobuf() throws IOException {
        CatalogMessage.SQLDecfloat.Builder builder = CatalogMessage.SQLDecfloat.newBuilder();
        builder.setIsNull(isNull);
        if (!isNull) {
            int scale = value.scale();
            if (scale < 0) {
                scale = 0;
                setValue(value.setScale(0));
            }
            BigInteger bi = value.unscaledValue();
            byte[] byteArray = bi.toByteArray();
            builder.setRawScale(scale);
            builder.setRawData(ByteString.copyFrom(byteArray));
        }
        CatalogMessage.DataValueDescriptor dvd =
                CatalogMessage.DataValueDescriptor.newBuilder()
                        .setType(CatalogMessage.DataValueDescriptor.Type.SQLDecfloat)
                        .setExtension(CatalogMessage.SQLDecfloat.sqlDecfloat, builder.build())
                        .build();
        return dvd;
    }

    @Override
    protected void readExternalNew(ObjectInput in) throws IOException {
        byte[] bs = ArrayUtil.readByteArray(in);
        CatalogMessage.SQLDecfloat decfloat = CatalogMessage.SQLDecfloat.parseFrom(bs);
        init(decfloat);
    }

    private void init(CatalogMessage.SQLDecfloat decfloat) {
        isNull = decfloat.getIsNull();
        if (!isNull) {
            int scale = decfloat.getRawScale();
            byte[] data = decfloat.getRawData().toByteArray();
            value = new BigDecimal(new BigInteger(data), scale);
        }
    }

    @Override
    protected void readExternalOld(ObjectInput in) throws IOException {
        try {
            if (in.readBoolean()) {
                setCoreValue(null);
                return;
            }

            value = ((BigDecimal) in.readObject()).round(MathContext.DECIMAL128);

            isNull = evaluateNull();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    public void readExternalFromArray(ArrayInputStream in) throws IOException, ClassNotFoundException {
        value = ((BigDecimal) in.readObject()).round(MathContext.DECIMAL128);
        isNull = evaluateNull();
    }

    /**
     * @see Storable#restoreToNull
     *
     */
    public void restoreToNull()
    {
        value = null;
        isNull = true;
    }


    /** @exception StandardException        Thrown on error */
    protected int typeCompare(DataValueDescriptor arg) throws StandardException
    {
        BigDecimal otherValue = SQLDecfloat.getBigDecimal(arg);
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
            return new SQLDecfloat(getBigDecimal());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @see DataValueDescriptor#getNewNull
     */
    public DataValueDescriptor getNewNull() {
        try {
            return new SQLDecfloat();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @see DataValueDescriptor#setValueFromResultSet
     *
     * @exception SQLException        Thrown on error
     */
    public void setValueFromResultSet(ResultSet resultSet, int colNumber,
                                      boolean isNullable)
        throws SQLException
    {
            setValue(resultSet.getBigDecimal(colNumber));
    }
    /**
        Set the value into a PreparedStatement.

        @exception SQLException Error setting value in PreparedStatement
    */
    public final void setInto(PreparedStatement ps, int position) throws SQLException {

        if (isNull()) {
            ps.setNull(position, com.splicemachine.db.iapi.reference.Types.DECFLOAT);
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
     *        when the String format is not recognized.
     */
    public void setValue(String theValue) throws StandardException
    {
        if (theValue == null)
        {
            restoreToNull();
        }
        else
        {
            try
            {
                theValue = theValue.trim();
                setValue(new BigDecimal(theValue, MathContext.DECIMAL128));
            } catch (NumberFormatException nfe)
            {
                throw invalidFormat();
            }
        }
    }

    /**
     * @see NumberDataValue#setValue
     *
     * @exception StandardException        Thrown on error
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
        value = new BigDecimal(theValue, MathContext.DECIMAL128);
        isNull = false;
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
                !(theValue instanceof BigDecimal) &&
                !(theValue instanceof Long))
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
        return TypeId.DECFLOAT_PRECEDENCE;
    }
    // END DataValueDescriptor interface

    private void setCoreValue(BigDecimal theValue)
    {
        if (theValue == null) {
            value = null;
        } else {
            value = theValue.round(MathContext.DECIMAL128);
        }
        isNull = evaluateNull();
    }

    private void setCoreValue(double theValue) {
        value = new BigDecimal(Double.toString(theValue), MathContext.DECIMAL128);
        isNull = false;
    }

    /**
     * Normalization method - this method may be called when putting
     * a value into a SQLDecimal, for example, when inserting into a SQLDecimal
     * column.  See NormalizeResultSet in execution.
     * <p>
     * Note that truncation is allowed on the decimal portion
     * of a numeric only.
     *
     * @param desiredType    The type to normalize the source column to
     * @param source        The value to normalize
     *
     * @throws StandardException                Thrown for null into
     *                                            non-nullable column, and for
     *                                            truncation error
     */
    public void normalize(
                DataTypeDescriptor desiredType,
                DataValueDescriptor source)
                        throws StandardException
    {
        setFrom(source);
    }


    /*
    ** SQL Operators
    */


    /**
     * This method implements the + operator for DECIMAL.
     *
     * @param addend1    One of the addends
     * @param addend2    The other addend
     * @param result    The result of a previous call to this method, null
     *                    if not called yet
     *
     * @return    A SQLDecimal containing the result of the addition
     *
     * @exception StandardException        Thrown on error
     */

    public NumberDataValue plus(NumberDataValue addend1,
                            NumberDataValue addend2,
                            NumberDataValue result)
                throws StandardException
    {
        if (result == null)
        {
            result = new SQLDecfloat();
        }

        if (addend1.isNull() || addend2.isNull())
        {
            result.setToNull();
            return result;
        }

        result.setBigDecimal(Objects.requireNonNull(SQLDecfloat.getBigDecimal(addend1)).add(SQLDecfloat.getBigDecimal(addend2), MathContext.DECIMAL128));
        return result;
    }

    /**
     * This method implements the - operator for "decimal - decimal".
     *
     * @param left    The value to be subtracted from
     * @param right    The value to be subtracted
     * @param result    The result of a previous call to this method, null
     *                    if not called yet
     *
     * @return    A SQLDecimal containing the result of the subtraction
     *
     * @exception StandardException        Thrown on error
     */

    public NumberDataValue minus(NumberDataValue left,
                            NumberDataValue right,
                            NumberDataValue result)
                throws StandardException
    {
        if (result == null)
        {
            result = new SQLDecfloat();
        }

        if (left.isNull() || right.isNull())
        {
            result.setToNull();
            return result;
        }

        result.setBigDecimal(Objects.requireNonNull(SQLDecfloat.getBigDecimal(left)).subtract(SQLDecfloat.getBigDecimal(right), MathContext.DECIMAL128));
        return result;
    }

    /**
     * This method implements the * operator for "double * double".
     *
     * @param left    The first value to be multiplied
     * @param right    The second value to be multiplied
     * @param result    The result of a previous call to this method, null
     *                    if not called yet
     *
     * @return    A SQLDecimal containing the result of the multiplication
     *
     * @exception StandardException        Thrown on error
     */

    public NumberDataValue times(NumberDataValue left,
                            NumberDataValue right,
                            NumberDataValue result)
                throws StandardException
    {
        if (result == null)
        {
            result = new SQLDecfloat();
        }

        if (left.isNull() || right.isNull())
        {
            result.setToNull();
            return result;
        }

        result.setBigDecimal(Objects.requireNonNull(SQLDecfloat.getBigDecimal(left)).multiply(SQLDecfloat.getBigDecimal(right), MathContext.DECIMAL128));
        return result;
    }

    /**
     * This method implements the / operator for BigDecimal/BigDecimal
     *
     * @param dividend    The numerator
     * @param divisor    The denominator
     * @param result    The result of a previous call to this method, null
     *                    if not called yet
     *
     * @return    A SQLDecimal containing the result of the division
     *
     * @exception StandardException        Thrown on error
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
     * @param dividend    The numerator
     * @param divisor    The denominator
     * @param result    The result of a previous call to this method, null
     *                    if not called yet
     * @param scale        The result scale, if < 0, calculate the scale according
     *                    to the actual values' sizes
     *
     * @return    A SQLDecimal containing the result of the division
     *
     * @exception StandardException        Thrown on error
     */

    public NumberDataValue divide(NumberDataValue dividend,
                             NumberDataValue divisor,
                             NumberDataValue result,
                             int scale)
                throws StandardException
    {
        if (result == null)
        {
            result = new SQLDecfloat();
        }

        if (dividend.isNull() || divisor.isNull())
        {
            result.setToNull();
            return result;
        }

        BigDecimal divisorBigDecimal = SQLDecfloat.getBigDecimal(divisor);

        if (divisorBigDecimal.compareTo(ZERO) == 0)
        {
            throw  StandardException.newException(SQLState.LANG_DIVIDE_BY_ZERO);
        }
        BigDecimal dividendBigDecimal = SQLDecfloat.getBigDecimal(dividend);

        /*
        ** Set the result scale to be either the passed in scale, whcih was
        ** calculated at bind time to be max(ls+rp-rs+1, 4), where ls,rp,rs
        ** are static data types' sizes, which are predictable and stable
        ** (for the whole result set column, eg.); otherwise dynamically
        ** calculates the scale according to actual values.  Beetle 3901
        */
        assert dividendBigDecimal != null;
        result.setBigDecimal(dividendBigDecimal.divide(
                                    divisorBigDecimal,
                                    MathContext.DECIMAL128));

        return result;
    }

    /**
     * This method implements the unary minus operator for double.
     *
     * @param result    The result of a previous call to this method, null
     *                    if not called yet
     *
     * @return    A SQLDecimal containing the result of the division
     *
     * @exception StandardException        Thrown on error
     */

    public NumberDataValue minus(NumberDataValue result)
                                    throws StandardException
    {
        if (result == null)
        {
            result = new SQLDecfloat();
        }

        if (this.isNull())
        {
            result.setToNull();
            return result;
        }

        result.setBigDecimal(Objects.requireNonNull(getBigDecimal()).negate(MathContext.DECIMAL128));
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
        return !isNull() && (Objects.requireNonNull(getBigDecimal()).compareTo(ZERO) < 0);
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

        return SQLDecfloat.getWholeDigits(localValue) + getDecimalValueScale();
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
        case com.splicemachine.db.iapi.reference.Types.DECFLOAT:
        case Types.DECIMAL:
            return ((BigDecimal) value.getObject()).round(MathContext.DECIMAL128);
        case Types.CHAR:
            try {
                return new BigDecimal(value.getString().trim(), MathContext.DECIMAL128);
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
        if (ONE.compareTo(decimalValue) > 0)
            return 0;
        return decimalValue.precision() - decimalValue.scale();
    }

    public Format getFormat() {
        return Format.DECFLOAT;
    }

    @Override
    public void read(Row row, int ordinal) throws StandardException {
        if (row.isNullAt(ordinal))
            setToNull();
        else {
            isNull = false;
            value = new BigDecimal(row.getString(ordinal), MathContext.DECIMAL128);
        }
    }

    @Override
    public StructField getStructField(String columnName) {
        return DataTypes.createStructField(columnName, DataTypes.StringType, true);
    }

    public void updateThetaSketch(UpdateSketch updateSketch) {
        updateSketch.update(this.value.toEngineeringString());
    }

    @Override
    public void setSparkObject(Object sparkObject) throws StandardException {
        if (sparkObject == null)
            setToNull();
        else {
            value = new BigDecimal(sparkObject.toString(), MathContext.DECIMAL128);
            setIsNull(false);
        }
    }
}
