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

import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.ArrayInputStream;
import com.splicemachine.db.iapi.services.loader.ClassInspector;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.cache.ClassSize;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl.Format;
import com.yahoo.sketches.theta.UpdateSketch;
import org.apache.commons.lang.SerializationUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;


/**
 * This contains an instance of a user-defined type, that is, a java object.
 *
 */

public class UserType extends DataType
						implements UserDataValue
{
	private Object	value = null;

	/*
	** DataValueDescriptor interface
	** (mostly implemented in DataType)
	*/

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( UserType.class);

    public int estimateMemoryUsage()
    {
        int sz = BASE_MEMORY_USAGE;
        if( null != value)
        {
            // Probably an underestimate. Examining each field value would be expensive
            // and would produce an overestimate when fields reference shared objects
            sz += ClassSize.estimateAndCatalogBase( value.getClass());
        }
        
        return sz;
    } // end of estimateMemoryUsage

	public String getString()
	{
		if (! isNull())
		{
			return value.toString();

		}
		else
		{
			return null;
		}
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public boolean getBoolean() throws StandardException
	{
		if (! isNull())
			if (value instanceof Boolean) return (Boolean) value;
		return super.getBoolean();
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public byte getByte() throws StandardException
	{
		if (! isNull())
			// REMIND: check for overflow and truncation
			if (value instanceof Number) return ((Number)value).byteValue();
		return super.getByte();
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public short getShort() throws StandardException
	{
		if (! isNull())
			// REMIND: check for overflow and truncation
			if (value instanceof Number) return ((Number)value).shortValue();
		return super.getShort();
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public int getInt() throws StandardException
	{
		if (! isNull())
			// REMIND: check for overflow and truncation
			if (value instanceof Number) return ((Number)value).intValue();
		return super.getInt();
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */

	public long getLong() throws StandardException
	{
		if (! isNull())
			// REMIND: check for overflow and truncation
			if (value instanceof Number) return ((Number)value).longValue();
		return super.getLong();
	}


	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public float getFloat() throws StandardException
	{
		if (! isNull())
			// REMIND: check for overflow
			if (value instanceof Number) return ((Number)value).floatValue();
		return super.getFloat();
	}


	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public double getDouble() throws StandardException
	{
		if (! isNull())
			// REMIND: check for overflow
			if (value instanceof Number) return ((Number)value).doubleValue();
		return super.getDouble();
	}

	/**
	 * @exception StandardException thrown on failure to convert
	 */
	public byte[] getBytes() throws StandardException
	{
		if (! isNull())
			if (value instanceof byte[]) return ((byte[])value);
		return super.getBytes();
	}

	/**

		@exception StandardException thrown on failure
	 */
	public Date	getDate( Calendar cal) throws StandardException
	{
		if (! isNull())
		{
			if (value instanceof Date) 
				return ((Date)value);
			else if (value instanceof Timestamp)
				return (new SQLTimestamp((Timestamp)value).getDate(cal));
		}
		return super.getDate(cal);
	}

	/**
		@exception StandardException thrown on failure
	 */
	public Time	getTime( Calendar cal) throws StandardException
	{
		if (! isNull())
		{
			if (value instanceof Time) 
				return ((Time)value);
			else if (value instanceof Timestamp)
				return (new SQLTimestamp((Timestamp)value).getTime(cal));
		}
		return super.getTime(cal);
	}

	/**
		@exception StandardException thrown on failure
	 */
	public Timestamp	getTimestamp( Calendar cal) throws StandardException
	{
		if (! isNull())
		{
			if (value instanceof Timestamp) 
				return ((Timestamp)value);
			else if (value instanceof Date)
				return (new SQLDate((Date)value).getTimestamp(cal));
			else if (value instanceof Time)
				return (new SQLTime((Time)value).getTimestamp(cal));
		}
		return super.getTimestamp(cal);
	}

	void setObject(Object theValue)
    {
        setValue( theValue );
    }
    
	public Object getObject()
	{
		return value;
	}

	@Override
	public Object getSparkObject() throws StandardException {
		if (value instanceof Serializable) {
			return SerializationUtils.serialize((Serializable) value);
		} else {
			return super.getSparkObject();
		}
	}

	public int getLength()
	{
		return TypeDescriptor.MAXIMUM_WIDTH_UNKNOWN;
	}

	/* this is for DataType's error generator */
	public String getTypeName()
	{
		return isNull() ? "JAVA_OBJECT" : ClassInspector.readableClassName(value.getClass());
	}
	
	/**
	 * Get the type name of this value,  overriding
	 * with the passed in class name (for user/java types).
	 */
	String getTypeName(String className)
	{
		return className;
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */

	/**
		Return my format identifier.

		@see com.splicemachine.db.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_USERTYPE_ID_V3;
	}

	/** 
		@exception IOException error writing data

	*/
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeBoolean(isNull());
		if (!isNull())
			out.writeObject(value);
	}

	/**
	 * @see java.io.Externalizable#readExternal
	 *
	 * @exception IOException	Thrown on error reading the object
	 * @exception ClassNotFoundException	Thrown if the class of the object
	 *										is not found
	 */
	public void readExternal(ObjectInput in) 
        throws IOException, ClassNotFoundException
	{
		boolean readIsNull = in.readBoolean();
		if (!readIsNull) {
			/* RESOLVE: Sanity check for right class */
			value = in.readObject();
			isNull = evaluateNull();
		} else {
			restoreToNull();
		}
	}
	public void readExternalFromArray(ArrayInputStream in) 
        throws IOException, ClassNotFoundException
	{
		/* RESOLVE: Sanity check for right class */
		value = in.readObject();
		isNull = evaluateNull();
	}

	/*
	 * DataValueDescriptor interface
	 */

    /** @see DataValueDescriptor#cloneValue */
    public DataValueDescriptor cloneValue(boolean forceMaterialization)
	{
		// Call constructor with all of our info
		return new UserType(value);
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new UserType();
	}
	/**
	 * @see com.splicemachine.db.iapi.services.io.Storable#restoreToNull
	 *
	 */

	public void restoreToNull()
	{
		value = null;
		isNull = evaluateNull();
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** 
	 * @see DataValueDescriptor#setValueFromResultSet 
	 *
	 * @exception SQLException		Thrown on error
	 */
	public void setValueFromResultSet(ResultSet resultSet, int colNumber,
									  boolean isNullable)
		throws SQLException
	{
			value = resultSet.getObject(colNumber);
			isNull = evaluateNull();
	}

	/**
	 * Orderable interface
	 *
	 *
	 * @see com.splicemachine.db.iapi.types.Orderable
	 *
	 * @exception StandardException thrown on failure
	 */
	public int compare(DataValueDescriptor other)
		throws StandardException
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
		 * thisNull otherNull	return
		 *	T		T		 	0	(this == other)
		 *	F		T		 	-1 	(this < other)
		 *	T		F		 	1	(this > other)
		 */
		if (thisNull || otherNull)
		{
			if (!thisNull)		// otherNull must be true
				return -1;
			if (!otherNull)		// thisNull must be true
				return 1;
			return 0;
		}

		/*
			Neither are null compare them 
		 */

		int comparison;

		try
		{
			comparison = ((java.lang.Comparable) value).compareTo(other.getObject());
		}
		catch (ClassCastException cce)
		{
			throw StandardException.newException(SQLState.LANG_INVALID_COMPARE_TO, 
						getTypeName(),
						ClassInspector.readableClassName(other.getObject().getClass()));
		}
		/*
		** compareTo() can return any negative number if less than, and
		** any positive number if greater than.  Change to -1, 0, 1.
		*/
		if (comparison < 0)
			comparison = -1;
		else if (comparison > 0)
			comparison = 1;

		return comparison;
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

		/* For usertypes and equal do some special processing when
		 * neither value is null.  (Superclass will handle comparison
		 * if either value is null.)
		 */
		if ( (op == ORDER_OP_EQUALS) &&
			(! this.isNull()) && (! other.isNull()) ) 
		{
			// if this object implements java.lang.Comparable (JDK1.2)
			// then we let the compareTo method handle equality
			// if it doesn't then we use the equals() method
			Object o = getObject();

			if (!(o instanceof java.lang.Comparable)) 
			{
				return o.equals(other.getObject());
			}
		}
		

		/* Do the comparison */
		return super.compare(op, other, orderedNulls, unknownRV);
	}

	/*
	** Class interface
	*/

	/*
	** Constructors
	*/
	/** no-arg constructor required by Formattable */
	public UserType() {
	}

	public UserType(Object value)
	{
		this.value = value;
		isNull = evaluateNull();
	}
	/**
	 * @see UserDataValue#setValue
	 *
	 */
	public void setValue(Object value)
	{
		this.value = value;
		isNull = evaluateNull();
	}
	protected void setFrom(DataValueDescriptor theValue) throws StandardException {

		setValue(theValue.getObject());
	}

	/**
	 * @see UserDataValue#setValue
	 *
	 */
	public void setBigDecimal(Number theValue)
	{
		// needed to allow serializable BigDecimal
		setValue((Object) theValue);
	}

	public void setValue(String theValue)
	{
		if (theValue == null)
		{
			value = null;
		}
		else
		{
			// Higher levels must have performed type checking for us.
			value = theValue;
		}
		isNull = evaluateNull();
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
									 left.compare(ORDER_OP_EQUALS, right, true, false));
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
									 !left.compare(ORDER_OP_EQUALS, right, true, false));
	}



	/*
	** String display of value
	*/

	public String toString()
	{
		if (isNull())
		{
			return "NULL";
		}
		else
		{
			return value.toString();
		}
	}

	/*
	 * Hash code
	 */
	public int hashCode()
	{
		if (isNull())
			return 0;
		return value.hashCode();
	}

	/** @see DataValueDescriptor#typePrecedence */
	public int	typePrecedence()
	{
		return TypeId.USER_PRECEDENCE;
	}

	/**
	 * Check if the value is null.  
	 *
	 * @return Whether or not value is logically null.
	 */
	private boolean evaluateNull()
	{
		return (value == null);
	}
	
	public Format getFormat() {
		return Format.USERTYPE;
	}

	@Override
	public void read(Row row, int ordinal) throws StandardException {
		if (row.isNullAt(ordinal))
			setToNull();
		else {
			isNull = false;
			Object object = row.get(ordinal);
			if (object instanceof byte[]) {
				value = SerializationUtils.deserialize((byte[]) object);
			} else {
				value = object;
			}
		}
	}

	@Override
	public StructField getStructField(String columnName) {
		return DataTypes.createStructField(columnName, DataTypes.BinaryType, true);
	}


	public void updateThetaSketch(UpdateSketch updateSketch) {
		if (!isNull())
		updateSketch.update(SerializationUtils.serialize(SerializationUtils.serialize((Serializable)value)));
	}

	@Override
	public void setSparkObject(Object sparkObject) throws StandardException {
		if (sparkObject instanceof byte[]) {
			value = SerializationUtils.deserialize((byte[]) sparkObject);
		} else {
			value = sparkObject;
		}
	}

}
