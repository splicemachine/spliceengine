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


import com.splicemachine.db.iapi.services.io.ArrayInputStream;
import com.splicemachine.db.iapi.services.io.Storable;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.cache.ClassSize;
import com.splicemachine.db.iapi.util.StringUtil;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
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
 * SQLBoolean satisfies the DataValueDescriptor
 * interfaces (i.e., DataType). It implements a boolean column, 
 * e.g. for * storing a column value; it can be specified
 * when constructed to not allow nulls. Nullability cannot be changed
 * after construction, as it affects the storage size and mechanism.
 * <p>
 * Because DataType is a subtype of DataType,
 * SQLBoolean can play a role in either a DataType/Row
 * or a DataType/Row, interchangeably.
 * <p>
 * We assume the store has a flag for nullness of the value,
 * and simply return a 0-length array for the stored form
 * when the value is null.
 * <p>
 * PERFORMANCE: There are likely alot of performance improvements
 * possible for this implementation -- it new's Integer
 * more than it probably wants to.
 */
public final class SQLBoolean
	extends DataType implements BooleanDataValue {
	public static final byte F = 0x01;
	public static final byte T = 0x02;
	/*
	 * DataValueDescriptor interface
	 * (mostly implemented in DataType)
	 */

	public boolean	getBoolean() {
		return value;
	}

	private static int makeInt(boolean b)
	{
		return (b?1:0);
	}

	/** 
	 * @see DataValueDescriptor#getByte 
	 */
	public byte	getByte() 
	{
		return (byte) makeInt(value);
	}

	/** 
	 * @see DataValueDescriptor#getShort 
	 */
	public short	getShort()
	{
		return (short) makeInt(value);
	}

	/** 
	 * @see DataValueDescriptor#getInt 
	 */
	public int	getInt()
	{
		return makeInt(value);
	}

	/** 
	 * @see DataValueDescriptor#getLong 
	 */
	public long	getLong()
	{
		return (long) makeInt(value);
	}

	/** 
	 * @see DataValueDescriptor#getFloat 
	 */
	public float	getFloat()
	{
		return (float) makeInt(value);
	}

	/** 
	 * @see DataValueDescriptor#getDouble 
	 */
	public double	getDouble()
	{
		return (double) makeInt(value);
	}

	/**
	 * Implementation for BOOLEAN type. Convert to a BigDecimal using long
	 */
	public int typeToBigDecimal()
	{
		return java.sql.Types.BIGINT;
	}
	public String	getString()
	{
		if (isNull())
			return null;
		else if (value == true)
			return "true";
		else
			return "false";
	}

	public Object	getObject()
	{
		if (isNull())
			return null;
		else
			return new Boolean(value);
	}

	public int	getLength()
	{
		return BOOLEAN_LENGTH;
	}

	// this is for DataType's error generator
	public String getTypeName()
	{
		return TypeId.BOOLEAN_NAME;
	}

    /**
     * Recycle this SQLBoolean object if possible. If the object is immutable,
     * create and return a new object.
     *
     * @return a new SQLBoolean if this object is immutable; otherwise, this
     * object with value set to null
     */
    public DataValueDescriptor recycle() {
            return new SQLBoolean();
    }

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */


	/**
		Return my format identifier.

		@see com.splicemachine.db.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_BOOLEAN_ID;
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeBoolean(isNull);
		out.writeBoolean(value);
	}

	/** @see java.io.Externalizable#readExternal */
	public void readExternal(ObjectInput in) throws IOException {
		isNull = in.readBoolean();
		value = in.readBoolean();
	}
	public void readExternalFromArray(ArrayInputStream in) throws IOException {
		isNull = in.readBoolean();
		value = in.readBoolean();
	}

	/**
	 * @see Storable#restoreToNull
	 *
	 */
	public void restoreToNull() {
		value = false;
		isNull = true;
	}

	/*
	 * Orderable interface
	 */

	/**
		@exception StandardException thrown on error
	 */
	public int compare(DataValueDescriptor other) throws StandardException
	{
		/* Use compare method from dominant type, negating result
		 * to reflect flipping of sides.
		 */
		if (typePrecedence() < other.typePrecedence())
		{
			return - (other.compare(this));
		}

		boolean thisNull, otherNull;
		thisNull = this.isNull();
		otherNull = other.isNull();

		/*
		 * thisNull otherNull thisValue thatValue return
		 *	T		T			X		X			0	(this == other)
		 *	F		T			X		X			-1 	(this > other)
		 *	T		F			X		X			1	(this < other)
		 *
		 *	F		F			T		T			0	(this == other)
		 *	F		F			T		F			1	(this > other)
		 *	F		F			F		T			-1	(this < other)
		 *	F		F			F		F			0	(this == other)
		 */
		if (thisNull || otherNull)
		{
			if (!thisNull)		// otherNull must be true
				return -1;
			if (!otherNull)		// thisNull must be true
				return 1;
			return 0;
		}

		/* neither are null, get the value */
		boolean thisValue;
		boolean otherValue = false;
		thisValue = this.getBoolean();

		otherValue = other.getBoolean();

		if (thisValue == otherValue)
			return 0;
		else if (thisValue && !otherValue)
			return 1;
		else
			return -1;
	}

	/**
		@exception StandardException thrown on error
	 */
	public boolean compare(int op,
						   DataValueDescriptor other,
						   boolean orderedNulls,
						   boolean unknownRV)
		throws StandardException
	{
		if (!orderedNulls)		// nulls are unordered
		{
			if (this.isNull() || other.isNull())
				return unknownRV;
		}
		/* Do the comparison */
		return super.compare(op, other, orderedNulls, unknownRV);
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#cloneValue */
	public DataValueDescriptor cloneValue(boolean forceMaterialization)
	{
		return new SQLBoolean(value, isNull);
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLBoolean();
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
			value = resultSet.getBoolean(colNumber);
			isNull = (isNullable && resultSet.wasNull());
	}
	/**
		Set the value into a PreparedStatement.

		@exception SQLException Error setting value in PreparedStatement
	*/
	public final void setInto(PreparedStatement ps, int position) throws SQLException {

		if (isNull()) {
			ps.setNull(position, java.sql.Types.BIT);
			return;
		}

		ps.setBoolean(position, value);
	}
	/*
	 * class interface
	 */

	/*
	 * constructors
	 */

	/* NOTE - other data types have both (type value) and (boolean nulls), 
	 * (value, nulls)
	 * We can't do both (boolean value) and (boolean nulls) here,
	 * so we'll skip over (boolean value) and have (Boolean value) so
	 * that we can support (boolean nulls).
	 */

	public SQLBoolean()
	{
		isNull = true;
	}

	public SQLBoolean(boolean val)
	{
		setValue(val);
	}
	public SQLBoolean(Boolean obj) {
		if (isNull = (obj == null))
			;
		else
			setValue(obj.booleanValue());
	}

    /* This constructor gets used for the cloneValue method */
	private SQLBoolean(boolean val, boolean isNull)
	{
		value = val;
		this.isNull = isNull;
	}

	/** @see BooleanDataValue#setValue */
	public void setValue(boolean theValue)
	{
		value = theValue;
		isNull = false;

	}

	public void setValue(Boolean theValue)
	{
		if (theValue == null)
		{
			value = false;
			isNull = true;
		}
		else
		{
			value = theValue.booleanValue();
			isNull = false;
		}

	}

	// REMIND: do we need this, or is long enough?
	public void setValue(byte theValue)
	{
		value = theValue != 0;
		isNull = false;

	}


	// REMIND: do we need this, or is long enough?
	public void setValue(short theValue)
	{
		value = theValue != 0;
		isNull = false;

	}


	// REMIND: do we need this, or is long enough?
	public void setValue(int theValue)
	{
		value = theValue != 0;
		isNull = false;

	}

	public void setValue(long theValue)
	{
		value = theValue != 0;
		isNull = false;

	}

	// REMIND: do we need this, or is double enough?
	public void setValue(float theValue)
	{
		value = theValue != 0;
		isNull = false;

	}

	public void setValue(double theValue)
	{
		value = theValue != 0;
		isNull = false;

	}

	public void setBigDecimal(Number bigDecimal) throws StandardException
	{
		if (bigDecimal == null)
		{
			value = false;
			isNull = true;
		}
		else
		{
			DataValueDescriptor tempDecimal = NumberDataType.ZERO_DECIMAL.getNewNull();
			tempDecimal.setBigDecimal(bigDecimal);
			value = NumberDataType.ZERO_DECIMAL.compare(tempDecimal) != 0;
			isNull = false;
		}

	}

	/**
	 * Set the value of this BooleanDataValue to the given String.
	 * String is trimmed and upcased.  If resultant string is not
	 * TRUE or FALSE, then an error is thrown.
	 *
	 * @param theValue	The value to set this BooleanDataValue to
	 *
	 * @exception StandardException Thrown on error
	 */
	public void setValue(String theValue)
		throws StandardException
	{
		if (theValue == null)
		{
			value = false;
			isNull = true;
		}
		else
		{
			/*
			** Note: cannot use getBoolean(String) here because
			** it doesn't trim, and doesn't throw exceptions.
			*/
			String cleanedValue = StringUtil.SQLToUpperCase(theValue.trim());
			if (cleanedValue.equals("TRUE") || cleanedValue.equals("T") || cleanedValue.equals("1")) {
				value = true;
                isNull = false;
			}
			else if (cleanedValue.equals("FALSE")  || cleanedValue.equals("F") || cleanedValue.equals("0")) {
				value = false;
                isNull = false;
			}
			else if (cleanedValue.equals("UNKNOWN")) {
				value = false;
                isNull = true;
			}
			else
			{ 
				throw invalidFormat();
			}
		}

	}

	/**
	 * @see DataValueDescriptor#setValue
	 */	
	void setObject(Object theValue)
	{
		setValue((Boolean) theValue);
	}
	
	protected void setFrom(DataValueDescriptor theValue) throws StandardException {
        if ( theValue instanceof SQLChar ) { setValue( theValue.getString() ); }
		else if ( theValue instanceof SQLBoolean ){ setValue(theValue.getBoolean()); }
        // DB-1331: When input value is VARCHAR, it is wrapped in a LazyStringDataValueDescriptor,
        // so we can not just check for instance of SQLVarchar. Nor can we check
        // instance of LazyStringDataValueDecriptor, because that's in splice machine
        // and here we are in Derby layer.
		else if ( theValue.getFormat() == Format.VARCHAR) {
			setValue(theValue.getString());
		}
        else
        {
            throw StandardException.newException
                ( SQLState.LANG_DATA_TYPE_SET_MISMATCH, theValue.getTypeName(), getTypeName() );

        }
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
		return truthValue(left,
							right,
							left.getBoolean() == right.getBoolean());
	}

	/**
	 * The <> operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the <>
	 * @param right			The value on the right side of the <>
	 *
	 * @return	A SQL boolean value telling whether the two parameters are
	 *			not equal
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue notEquals(DataValueDescriptor left,
							 DataValueDescriptor right)
				throws StandardException
	{
		return truthValue(left,
							right,
							left.getBoolean() != right.getBoolean());
	}

	/**
	 * The < operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the <
	 * @param right			The value on the right side of the <
	 *
	 * @return	A SQL boolean value telling whether the left operand is
	 *			less than the right operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue lessThan(DataValueDescriptor left,
							 DataValueDescriptor right)
				throws StandardException
	{
		/* We must call getBoolean() on both sides in order
		 * to catch any invalid casts.
		 */
		boolean leftBoolean = left.getBoolean();
		boolean rightBoolean = right.getBoolean();
		/* By convention, false is less than true */
		return truthValue(left,
							right,
							leftBoolean == false && rightBoolean == true);
	}

	/**
	 * The > operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the >
	 * @param right			The value on the right side of the >
	 *
	 * @return	A SQL boolean value telling whether the left operand is
	 *			greater than the right operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue greaterThan(DataValueDescriptor left,
							 DataValueDescriptor right)
				throws StandardException
	{
		/* We must call getBoolean() on both sides in order
		 * to catch any invalid casts.
		 */
		boolean leftBoolean = left.getBoolean();
		boolean rightBoolean = right.getBoolean();
		/* By convention, true is greater than false */
		return truthValue(left,
							right,
							leftBoolean == true && rightBoolean == false);
	}

	/**
	 * The <= operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the <=
	 * @param right			The value on the right side of the <=
	 *
	 * @return	A SQL boolean value telling whether the left operand is
	 *			less than or equal to the right operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue lessOrEquals(DataValueDescriptor left,
							 DataValueDescriptor right)
				throws StandardException
	{
		/* We must call getBoolean() on both sides in order
		 * to catch any invalid casts.
		 */
		boolean leftBoolean = left.getBoolean();
		boolean rightBoolean = right.getBoolean();
		/* By convention, false is less than true */
		return truthValue(left,
							right,
							leftBoolean == false || rightBoolean == true);
	}

	/**
	 * The >= operator as called from the language module, as opposed to
	 * the storage module.
	 *
	 * @param left			The value on the left side of the >=
	 * @param right			The value on the right side of the >=
	 *
	 * @return	A SQL boolean value telling whether the left operand is
	 *			greater than or equal to the right operand
	 *
	 * @exception StandardException		Thrown on error
	 */

	public BooleanDataValue greaterOrEquals(DataValueDescriptor left,
							 DataValueDescriptor right)
				throws StandardException
	{
		/* We must call getBoolean() on both sides in order
		 * to catch any invalid casts.
		 */
		boolean leftBoolean = left.getBoolean();
		boolean rightBoolean = right.getBoolean();
		/* By convention, true is greater than false */
		return truthValue(left,
							right,
							leftBoolean == true || rightBoolean == false);
	}

	/**
	 * The AND operator.  This implements SQL semantics for AND with unknown
	 * truth values - consult any standard SQL reference for an explanation.
	 *
	 * @param otherValue	The other boolean to AND with this one
	 *
	 * @return	this AND otherValue
	 *
	 */

	public BooleanDataValue and(BooleanDataValue otherValue)
	{
		/*
		** Catch those cases where standard SQL null semantics don't work.
		*/
		if (this.equals(false) || otherValue.equals(false))
		{
			return BOOLEAN_FALSE;
		}
		else
		{
			return truthValue(this,
							otherValue,
							this.getBoolean() && otherValue.getBoolean());
		}
	}

	/**
	 * The OR operator.  This implements SQL semantics for OR with unknown
	 * truth values - consult any standard SQL reference for an explanation.
	 *
	 * @param otherValue	The other boolean to OR with this one
	 *
	 * @return	this OR otherValue
	 *
	 */

	public BooleanDataValue or(BooleanDataValue otherValue)
	{
		/*
		** Catch those cases where standard SQL null semantics don't work.
		*/
		if (this.equals(true) || otherValue.equals(true))
		{
			return BOOLEAN_TRUE;
		}
		else
		{
			return truthValue(this,
							otherValue,
							this.getBoolean() || otherValue.getBoolean());
		}
	}

	/**
	 * The SQL IS operator - consult any standard SQL reference for an explanation.
	 *
	 *	Implements the following truth table:
	 *
	 *	         otherValue
	 *	        | TRUE    | FALSE   | UNKNOWN
	 *	this    |----------------------------
	 *	        |
	 *	TRUE    | TRUE    | FALSE   | FALSE
	 *	FALSE   | FALSE   | TRUE    | FALSE
	 *	UNKNOWN | FALSE   | FALSE   | TRUE
	 *
	 *
	 * @param otherValue	BooleanDataValue to compare to. May be TRUE, FALSE, or UNKNOWN.
	 *
	 * @return	whether this IS otherValue
	 *
	 */
	public BooleanDataValue is(BooleanDataValue otherValue)
	{
		if ( this.equals(true) && otherValue.equals(true) )
		{ return BOOLEAN_TRUE; }

		if ( this.equals(false) && otherValue.equals(false) )
		{ return BOOLEAN_TRUE; }

		if ( this.isNull() && otherValue.isNull() )
		{ return BOOLEAN_TRUE; }

		return BOOLEAN_FALSE;
	}

	/**
	 * Implements NOT IS. This reverses the sense of the is() call.
	 *
	 *
	 * @param otherValue	BooleanDataValue to compare to. May be TRUE, FALSE, or UNKNOWN.
	 *
	 * @return	NOT( this IS otherValue )
	 *
	 */
	public BooleanDataValue isNot(BooleanDataValue otherValue)
	{
		BooleanDataValue	isValue = is( otherValue );

		if ( isValue.equals(true) ) { return BOOLEAN_FALSE; }
		else { return BOOLEAN_TRUE; }
	}

	/**
	 * Throw an exception with the given SQLState if this BooleanDataValue
	 * is false. This method is useful for evaluating constraints.
	 *
	 * @param sqlState		The SQLState of the exception to throw if
	 *						this SQLBoolean is false.
	 * @param tableName		The name of the table to include in the exception
	 *						message.
	 * @param constraintName	The name of the failed constraint to include
	 *							in the exception message.
	 *
	 * @return	this
	 *
	 * @exception	StandardException	Thrown if this BooleanDataValue
	 *									is false.
	 */
	public BooleanDataValue throwExceptionIfFalse(
									String sqlState,
									String tableName,
									String constraintName)
							throws StandardException
	{
		if ( ( ! isNull() ) && (value == false) )
		{
			throw StandardException.newException(sqlState,
												tableName,
												constraintName);
		}

		return this;
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.BOOLEAN_PRECEDENCE;
	}

	/*
	** Support functions
	*/

	/**
	 * Return the SQL truth value for a comparison.
	 *
	 * This method first looks at the operands - if either is null, it
	 * returns the unknown truth value.  This implements "normal" SQL
	 * null semantics, where if any operand is null, the result is null.
	 * Note that there are cases where these semantics are incorrect -
	 * for example, NULL AND FALSE is supposed to be FALSE, not NULL
	 * (the NULL truth value is the same as the UNKNOWN truth value).
	 *
	 * If neither operand is null, it returns a static final variable
	 * containing the SQLBoolean truth value.  It returns different values
	 * depending on whether the truth value is supposed to be nullable.
	 *
	 * This method always returns a pre-allocated static final SQLBoolean.
	 * This is practical because there are so few possible return values.
	 * Using pre-allocated values allows us to avoid constructing new
	 * SQLBoolean values during execution.
	 *
	 * @param leftOperand	The left operand of the binary comparison
	 * @param rightOperand	The right operand of the binary comparison
	 * @param truth			The truth value of the comparison
	 *
	 * @return	A SQLBoolean containing the desired truth value.
	 */

	public static SQLBoolean truthValue(
								DataValueDescriptor leftOperand,
								DataValueDescriptor rightOperand,
								boolean truth)
	{
		/* Return UNKNOWN if either operand is null */
		if (leftOperand.isNull() || rightOperand.isNull())
		{
			return unknownTruthValue();
		}

		/* Return the appropriate SQLBoolean for the given truth value */
		if (truth == true)
		{
			return BOOLEAN_TRUE;
		}
		else
		{
			return BOOLEAN_FALSE;
		}
	}

    /**
     * same as above, but takes a Boolean, if it is null, unknownTruthValue is returned
     */
	public static SQLBoolean truthValue(
								DataValueDescriptor leftOperand,
								DataValueDescriptor rightOperand,
								Boolean truth)
	{
		/* Return UNKNOWN if either operand is null */
		if (leftOperand.isNull() || rightOperand.isNull() || truth==null)
		{
			return unknownTruthValue();
		}

		/* Return the appropriate SQLBoolean for the given truth value */
		if (truth == Boolean.TRUE)
		{
			return BOOLEAN_TRUE;
		}
		else
		{
			return BOOLEAN_FALSE;
		}
	}

	/**
	 * Get a truth value.
	 *
	 * @param value	The value of the SQLBoolean
	 *
 	 * @return	A SQLBoolean with the given truth value
	 */
	public static SQLBoolean truthValue(boolean value)
	{
		/*
		** Return the non-nullable versions of TRUE and FALSE, since they
		** can never be null.
		*/
		if (value == true)
			return BOOLEAN_TRUE;
		else
			return BOOLEAN_FALSE;
	}

	/**
	 * Return an unknown truth value.  Check to be sure the return value is
	 * nullable.
	 *
	 * @return	A SQLBoolean representing the UNKNOWN truth value
	 */
	public static SQLBoolean unknownTruthValue()
	{
		return UNKNOWN;
	}

	/**
	 * Return a false truth value.
	 *
	 *
	 * @return	A SQLBoolean representing the FALSE truth value
	 */
	public static SQLBoolean falseTruthValue()
	{
		return BOOLEAN_FALSE;
	}

	/**
	 * Return a true truth value.
	 *
	 *
	 * @return	A SQLBoolean representing the TRUE truth value
	 */
	public static SQLBoolean trueTruthValue()
	{
		return BOOLEAN_TRUE;
	}
	
	/**
	 * Determine whether this SQLBoolean contains the given boolean value.
	 *
	 * This method is used by generated code to determine when to do
	 * short-circuiting for an AND or OR.
	 *
	 * @param val	The value to look for
	 *
	 * @return	true if the given value equals the value in this SQLBoolean,
	 *			false if not
	 */

	public boolean equals(boolean val)
	{
		if (isNull())
			return false;
		else
			return value == val;
	}
	
	/**
	 * Return an immutable BooleanDataValue with the same value as this.
	 * @return An immutable BooleanDataValue with the same value as this.
	 */
	public BooleanDataValue getImmutable()
	{
		if (isNull())
			return SQLBoolean.UNKNOWN;
		
		return value ? SQLBoolean.BOOLEAN_TRUE : SQLBoolean.BOOLEAN_FALSE;
	}

	/*
	 * String display of value
	 */

	public String toString()
	{
		if (isNull())
			return "NULL";
		else if (value == true)
			return "true";
		else
			return "false";
	}

	/*
	 * Hash code
	 */
	public int hashCode()
	{
		if (isNull())
		{
			return -1;
		}

		return (value) ? 1 : 0;
	}

	/*
	 * useful constants...
	 */
	static final int BOOLEAN_LENGTH		= 1;	// must match the number of bytes written by DataOutput.writeBoolean()

	private static final SQLBoolean BOOLEAN_TRUE = new SQLBoolean(true);
	private static final SQLBoolean BOOLEAN_FALSE = new SQLBoolean(false);
	static final SQLBoolean UNKNOWN = new SQLBoolean();

	/* Static initialization block */
	static
	{
	}

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( SQLBoolean.class);

    public int estimateMemoryUsage()
    {
        return BASE_MEMORY_USAGE;
    }

	/*
	 * object state
	 */
	private boolean value;
	public Format getFormat() {
		return Format.BOOLEAN;
	}
	/**
	 *
	 * Write to a Project Tungsten UnsafeRow Format.
	 *
	 * @see UnsafeRowWriter#write(int, boolean)
	 *
	 * @param unsafeRowWriter
	 * @param ordinal
     */
	@Override
	public void write(UnsafeRowWriter unsafeRowWriter, int ordinal) {
		if (isNull())
			unsafeRowWriter.setNullAt(ordinal);
		else {
			isNull = false;
			unsafeRowWriter.write(ordinal, value);
		}
	}

	/**
	 *
	 * Read from a Project Tungsten UnsafeRow Format.
	 *
	 * @see UnsafeRow#getBoolean(int)
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
			value = unsafeRow.getBoolean(ordinal);
		}
	}

	@Override
	public void read(Row row, int ordinal) throws StandardException {
		if (row.isNullAt(ordinal))
			setToNull();
		else {
			isNull = false;
			value = row.getBoolean(ordinal);
		}
	}


	/**
	 *
	 * Gets the encoded key length.  1 if null else 2.
	 *
	 * @return
	 * @throws StandardException
     */
	@Override
	public int encodedKeyLength() throws StandardException {
		return isNull()?1:2;
	}

	/**
	 *
	 * Performs null check and then encodes.
	 *
	 * @see OrderedBytes#encodeInt8(PositionedByteRange, byte, Order)
	 *
	 * @param src
	 * @param order
	 * @throws StandardException
     */
	@Override
	public void encodeIntoKey(PositionedByteRange src, Order order) throws StandardException {
		if (isNull())
			OrderedBytes.encodeNull(src,order);
		else
			OrderedBytes.encodeInt8(src, value ? T : F, order);
	}

	/**
	 *
	 * Performs null check and then decodes.
	 *
	 * @see OrderedBytes#decodeInt8(PositionedByteRange)
	 *
	 * @param src
	 * @throws StandardException
     */
	@Override
	public void decodeFromKey(PositionedByteRange src) throws StandardException {
		if (OrderedBytes.isNull(src))
			setToNull();
		else
			value = OrderedBytes.decodeInt8(src)==T?true:false;
	}

	@Override
	public StructField getStructField(String columnName) {
		return DataTypes.createStructField(columnName, DataTypes.BooleanType, true);
	}

	@Override
	public void updateThetaSketch(UpdateSketch updateSketch) {
		updateSketch.update(value?new byte[]{T}:new byte[]{F});
	}
}
