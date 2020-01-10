/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.iapi.types;

import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.cache.ClassSize;
import com.splicemachine.db.iapi.services.io.ArrayInputStream;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl.Format;
import com.yahoo.sketches.theta.UpdateSketch;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.util.hashing.MurmurHash3;

import javax.ws.rs.NotSupportedException;
import java.io.*;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SQLArray extends DataType implements ArrayDataValue {
	protected DataValueDescriptor[] value;
	protected DataValueDescriptor type;
	protected int baseType;
	protected String baseTypeName;

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( SQLArray.class);

    public int estimateMemoryUsage()
    {
        int sz = BASE_MEMORY_USAGE;
        if( null != value) {
            for (DataValueDescriptor aValue : value) {
                sz += aValue.estimateMemoryUsage();
            }
		}
        return sz;
    } // end of estimateMemoryUsage

	public void setType(DataValueDescriptor type) {
		this.type = type;
	}

	/*
	** DataValueDescriptor interface
	** (mostly implemented in DataType)
	*/

	@Override
	public void setValue(byte[] theValue) throws StandardException {
		setIsNull(false);
		ObjectInputStream input = null;
		try {
			ByteArrayInputStream stream = new ByteArrayInputStream(theValue);
			input = new ObjectInputStream(stream);
			if (input.readBoolean()) {
				value = new DataValueDescriptor[input.readInt()];
				for (int i = 0; i< value.length; i++) {
					value[i] = (DataValueDescriptor) input.readObject();
				}
			}
		} catch (Exception e) {
			throw StandardException.plainWrapException(e);
		} finally {
			try {
				if (input != null) {
					input.close();
				}
			} catch (Exception ignored) {}
		}

	}



	@Override
	public byte[] getBytes() throws StandardException {
		ObjectOutput output = null;
		try {
			ByteArrayOutputStream stream = new ByteArrayOutputStream();
			output = new ObjectOutputStream(stream);
			output.writeBoolean(value !=null);
			if (value != null) {
				output.writeInt(value.length);
                for (DataValueDescriptor aValue : value) {
                    output.writeObject(aValue);
                }
			}
			output.flush();
			return stream.toByteArray();
		} catch (Exception e) {
			throw StandardException.plainWrapException(e);
		} finally {
			try {
				if (output != null) {
					output.close();
				}
			} catch (Exception ignored) {}
		}

	}


	public String getString() {
		return value !=null?Arrays.toString(value):null;
	}

	public Object getObject() {
		return value;
	}

	protected void setFrom(DataValueDescriptor theValue) throws StandardException {
		if (theValue.isNull())
			setToNull();
		else
			setValue((DataValueDescriptor[]) theValue.getObject());
	}

	public int getLength()
	{
		return TypeDescriptor.MAXIMUM_WIDTH_UNKNOWN;
	}

	/* this is for DataType's error generator */
	public String getTypeName()
	{
		return TypeId.ARRAY_NAME;
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */


	/**
		Return my format identifier.

		@see com.splicemachine.db.iapi.services.io.TypedFormat#getTypeFormatId
	*/


	public int getTypeFormatId() {
		return StoredFormatIds.SQL_ARRAY_ID;
	}

	private boolean evaluateNull()
	{
		return (value == null);
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeBoolean(type!=null);
		if (type!=null)
			out.writeObject(type);
		out.writeBoolean(value != null);
        if (value != null) {
			out.writeInt(value.length);
            for (DataValueDescriptor aValue : value) out.writeObject(aValue);
        }
	}

	/**
	 * @see java.io.Externalizable#readExternal
	 *
	 * @exception IOException	Thrown on error reading the object
	 * @exception ClassNotFoundException	Thrown if the class of the object
	 *										read from the stream can't be found
	 *										(not likely, since it's supposed to
	 *										be SQLRef).
	 */
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		if (in.readBoolean())
			type = (DataValueDescriptor) in.readObject();
        boolean nonNull = in.readBoolean();
		if (nonNull) {
			setIsNull(false);
			value = new DataValueDescriptor[in.readInt()];
			for (int i =0; i< value.length; i++) {
				value[i] = (DataValueDescriptor) in.readObject();
			}
        } else {
			setIsNull(true);
		}
	}
	public void readExternalFromArray(ArrayInputStream in) throws IOException, ClassNotFoundException {
		if (in.readBoolean())
			type = (DataValueDescriptor) in.readObject();
		boolean nonNull = in.readBoolean();
		if (nonNull) {
			value = new DataValueDescriptor[in.readInt()];
			for (int i =0; i< value.length; i++) {
				value[i] = (DataValueDescriptor) in.readObject();
			}
		}
	}

	/**
	 * @see com.splicemachine.db.iapi.services.io.Storable#restoreToNull
	 */

	public void restoreToNull() {
		value = null;
		isNull = true;
	}

	/*
	** Orderable interface
	*/

	/** @exception StandardException	Thrown on error */
	public boolean compare(int op,
						   DataValueDescriptor other,
						   boolean orderedNulls,
						   boolean unknownRV)
					throws StandardException {

		int result = compare(other);
		switch(op) {
			case ORDER_OP_LESSTHAN:
				return (result < 0);   // this <  other
			case ORDER_OP_EQUALS:
				return (result == 0);  // this == other
			case ORDER_OP_LESSOREQUALS:
				return (result <= 0);  // this <= other
			// flipped operators
			case ORDER_OP_GREATERTHAN:
				return (result > 0);   // this > other
			case ORDER_OP_GREATEROREQUALS:
				return (result >= 0);  // this >= other
			default:
				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT("Invalid Operator");
				return false;
		}

	}

	/** @exception StandardException	Thrown on error */
	public int compare(DataValueDescriptor other) throws StandardException {
		boolean thisNull, otherNull;

		thisNull = this.isNull();
		otherNull = other.isNull();

		/*
		 * thisNull otherNull	return
		 *	T		T		 	0	(this == other)
		 *	F		T		 	-1 	(this > other)
		 *	T		F		 	1	(this < other)
		 */
		if (thisNull || otherNull) {
			if (!thisNull)		// otherNull must be true
				return -1;
			if (!otherNull)		// thisNull must be true
				return 1;
			return 0;
		}

		if (other instanceof SQLArray) {
			DataValueDescriptor[] oArray = ((SQLArray) other).value;
			for (int i =0; i< this.value.length;i++) {
				if (oArray.length < i)
					return -1;
				int returnValue = value[i].compare(oArray[i]);
				if (returnValue != 0)
					return returnValue;
			}
			if (oArray.length > value.length)
				return 1;
			return 0;
        }
		else {
            throw StandardException.newException("cannot compare SQLArray with " + other.getClass());
        }
	}

	/*
	 * DataValueDescriptor interface
	 */

    /** @see DataValueDescriptor#cloneValue */
    public DataValueDescriptor cloneValue(boolean forceMaterialization)
	{
		/* In order to avoid a throws clause nightmare, we only call
		 * the constructors which do not have a throws clause.
		 *
		 * Clone the underlying RowLocation, if possible, so that we
		 * don't clobber the value in the clone.
		 */
		SQLArray array = null;
		if (value == null) {
			array = new SQLArray();

		}
		else {
			DataValueDescriptor[] values = new DataValueDescriptor[value.length];
			for (int i =0; i< values.length; i++) {
				values[i] = value[i].cloneValue(false);
			}
			array = new SQLArray(values);
		}
		array.setType(type);
		return array;
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		SQLArray array = new SQLArray();
		array.setType(type);
		array.setToNull();
		return array;
	}

	/**
	 * @see DataValueDescriptor#setValueFromResultSet
	 *
	 */
	public void setValueFromResultSet(ResultSet resultSet, int colNumber,
									  boolean isNullable) throws SQLException {
		Array array = resultSet.getArray(colNumber);
		int type = array.getBaseType();
		throw new NotSupportedException("still need to implement " + array + " : " + type);
	}
	public void setInto(PreparedStatement ps, int position)  throws SQLException {
		if (isNull()) {
			ps.setNull(position, Types.ARRAY);
			return;
		}
//		ps.setArray(position, value);
		throw new UnsupportedOperationException("setInto an array ");
	}

	/*
	** Class interface
	*/

	/*
	** Constructors
	*/

	public SQLArray() {}

	public SQLArray(DataValueDescriptor[] dvds) {
		setValue(dvds);
	}

    @Override
	public void setValue(DataValueDescriptor[] dvds) {
		value = dvds;
		if (type == null && dvds != null && dvds.length > 0)
			type = dvds[0].cloneValue(true);
		isNull = evaluateNull();
	}


	/*
	** String display of value
	*/

	public String toString() {
		if (value == null)
			return "NULL";
		else
			return Arrays.toString(value);
	}
	public Format getFormat() {
		return Format.ARRAY;
	}

	@Override
	public void read(Row row, int ordinal) throws StandardException {
		if (row.isNullAt(ordinal))
			setToNull();
		else {
			assert type != null:"type cannot be null when reading from Spark";
			setIsNull(false);
			List list = row.getList(ordinal);
			value = new DataValueDescriptor[list.size()];
			for (int i =0 ; i< value.length; i++) {
				value[i] = type.cloneValue(true);
				value[i].setSparkObject(list.get(i));
			}
		}
	}


	@Override
	public StructField getStructField(String columnName) {
		if (type == null)
			throw new RuntimeException("type cannot be null");
		return DataTypes.createStructField(columnName, DataTypes.createArrayType(type.getStructField("co").dataType()), true);
	}

	public void updateThetaSketch(UpdateSketch updateSketch) {
		try {
			updateSketch.update(getBytes());
		} catch (StandardException se) {
			throw new RuntimeException(se);
		}
	}

	@Override
	public DataValueDescriptor arrayElement(int element, DataValueDescriptor valueToSet) throws StandardException {
		if (value == null || value.length <= element || value[element] == null) {
			valueToSet.setToNull();
			return valueToSet;
		}
		else {
			valueToSet.isNotNull();
			valueToSet.setValue(value[element]);
			return valueToSet;
		}
	}

	@Override
	public String getBaseTypeName() throws SQLException {
		return baseTypeName;
	}

	public void setBaseTypeName(String baseTypeName) {
		this.baseTypeName = baseTypeName;
	}

	/**
	 *
	 * jdbc base type.
	 *
	 * @return
	 * @throws SQLException
     */
	@Override
	public int getBaseType() throws SQLException {
		return baseType;
	}

	/**
	 *
	 * jdbc base type.
	 *
	 * @return
	 * @throws SQLException
	 */
	public void setBaseType(int baseType) {
		this.baseType = baseType;
	}

	@Override
	public Object getArray() throws SQLException {
		return value;
	}

	@Override
	public Object getArray(Map<String, Class<?>> map) throws SQLException {
		return getArray();
	}

	@Override
	public Object getArray(long index, int count) throws SQLException {
		return Arrays.<DataValueDescriptor>copyOfRange(value,(int)index,count);
	}

	@Override
	public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
		return getArray(index,count);
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		throw new UnsupportedOperationException("Not Supported");
	}

	@Override
	public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
		throw new UnsupportedOperationException("Not Supported");
	}

	@Override
	public ResultSet getResultSet(long index, int count) throws SQLException {
		throw new UnsupportedOperationException("Not Supported");
	}

	@Override
	public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
		throw new UnsupportedOperationException("Not Supported");
	}

	@Override
	public void free() throws SQLException {
		// Free as a bird... (No Locator on our implementation)
	}

	@Override
	public Object getSparkObject() throws StandardException {
		if (value == null)
			return null;
		Object[] items = new Object[value.length];
		for (int i = 0; i< value.length; i++) {
			if (value[i] == null || value[i].isNull())
				continue;
			items[i] = value[i].getSparkObject();
		}
		return items;
	}

	@Override
	public void setSparkObject(Object sparkObject) throws StandardException {

	}

	public int hashCode() {
		if (value.length > 0)
			return MurmurHash3.arrayHashing().hash(value);
		else
			return 0;
	}

}
