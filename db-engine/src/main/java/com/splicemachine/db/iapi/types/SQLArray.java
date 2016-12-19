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
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
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
import com.splicemachine.db.iapi.services.io.TypedFormat;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl.Format;
import com.yahoo.sketches.theta.UpdateSketch;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import javax.ws.rs.NotSupportedException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.*;
import java.util.Arrays;
import java.util.Map;

public class SQLArray extends DataType implements DataValueDescriptor {
	protected DataValueDescriptor[] values;
	protected int typeFormatId;
    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( SQLArray.class);

    public int estimateMemoryUsage() {
        int sz = BASE_MEMORY_USAGE;
        if( null != values) {
			for (DataValueDescriptor dvd: values)
			if (dvd != null)
				sz += dvd.estimateMemoryUsage();
		}
        return sz;
    } // end of estimateMemoryUsage

	/*
	** DataValueDescriptor interface
	** (mostly implemented in DataType)
	*/

	public String getString() {
		return (values != null && values.length>0)?Arrays.toString(values):null;
	}

	public Object getObject()
	{
		return values;
	}

	protected void setFrom(DataValueDescriptor theValue) throws StandardException {

		if (theValue.isNull())
			setToNull();
		else
			setValue(theValue.getObject());
	}

	public int getLength() {
		return values!=null?values.length:0;
	}

	/* this is for DataType's error generator */
	public String getTypeName() {
		return TypeId.REF_NAME;
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */


	/**
		Return my format identifier.

		@see com.splicemachine.db.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return typeFormatId;
	}

	private final boolean evaluateNull() {
		return (values == null);
	}

	public void writeExternal(ObjectOutput out) throws IOException {

        out.writeBoolean(values != null);
		if (values!=null) {
			out.writeInt(values.length);
			for (DataValueDescriptor value: values)
				out.writeObject(value);
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
        boolean nonNull = in.readBoolean();
        if (nonNull) {
			values = new DataValueDescriptor[in.readInt()];
			for (int i = 0; i< values.length;i++) {
				values[i] = (DataValueDescriptor) in.readObject();
			}
        }
	}
	public void readExternalFromArray(ArrayInputStream in) throws IOException, ClassNotFoundException {
		boolean nonNull = in.readBoolean();
		if (nonNull) {
			values = new DataValueDescriptor[in.readInt()];
			for (int i = 0; i< values.length;i++) {
				values[i] = (DataValueDescriptor) in.readObject();
			}
		}
	}

	/**
	 * @see com.splicemachine.db.iapi.services.io.Storable#restoreToNull
	 */

	public void restoreToNull() {
		values = null;
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
		throw StandardException.newException("cannot compare SQLArray");
	}

	/** @exception StandardException	Thrown on error */
	public int compare(DataValueDescriptor other) throws StandardException {
		throw StandardException.newException("cannot compare SQLArray");
	}

	/*
	 * DataValueDescriptor interface
	 */

    /** @see DataValueDescriptor#cloneValue */
    public DataValueDescriptor cloneValue(boolean forceMaterialization) {
		if (!forceMaterialization)
			return new SQLArray(values);
		if (values == null)
			return new SQLArray(null);
		DataValueDescriptor[] newValues = new DataValueDescriptor[values.length];
		for (int i =0 ;i< values.length;i++) {
			newValues[i] = (values[i] == null)?null:values[i].cloneValue(forceMaterialization);
		}
		return new SQLArray(newValues);
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLArray();
	}

	/**
	 * @see DataValueDescriptor#setValueFromResultSet
	 *
	 */
	public void setValueFromResultSet(ResultSet resultSet, int colNumber,
									  boolean isNullable) {
		try {
			Array array = resultSet.getArray(colNumber);
			values = (DataValueDescriptor[]) array.getArray();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void setInto(PreparedStatement ps, int position)  {
		throw new NotSupportedException("Not Implemented");
	}

	/*
	** Class interface
	*/

	/*
	** Constructors
	*/

	public SQLArray() {
	}

	public SQLArray(DataValueDescriptor[] values) {
		setValue(values);
	}

	public void setValue(DataValueDescriptor[] values) {
		this.values = values;
		isNull = evaluateNull();
	}

	/*
	** String display of value
	*/

	public String toString() {
		String string = getString();
		return string==null?"NULL":string;
	}

	public Format getFormat() {
		return Format.REF;
	}

	/**
	 *
	 * Write into a Project Tungsten format (UnsafeRow).  This calls the
	 * reference's write method.
	 *
	 *
	 * @param unsafeRowWriter
	 * @param ordinal
	 * @throws StandardException
     */
	public void write(UnsafeRowWriter unsafeRowWriter, int ordinal) throws StandardException {
		if (values == null)
			unsafeRowWriter.setNullAt(ordinal);
		BufferHolder primaryHolder = unsafeRowWriter.holder();
		UnsafeRowWriter arrayDefWriter = new UnsafeRowWriter(primaryHolder,2);
		arrayDefWriter.write(0,values.length);
		BufferHolder arrayHolder = arrayDefWriter.holder();
		UnsafeRowWriter arrayValuesWriter = new UnsafeRowWriter(arrayHolder,values.length);
		for (int i = 0; i< values.length; i++) {
			if (values[i] == null)
				arrayValuesWriter.setNullAt(i);
			else {
				values[i].write(arrayValuesWriter,i);
			}
		}
	}

	/**
	 *
	 * Read into a Project Tungsten format.  This calls the Reference's
	 * read method.
	 *
	 * @param unsafeRow
	 * @param ordinal
	 * @throws StandardException
     */
	@Override
	public void read(UnsafeRow unsafeRow, int ordinal) throws StandardException {
		/*
		if (unsafeRow.isNullAt(ordinal) {
			this.setIsNull(true);
		}
		else {
			unsafeRow.getStruct(ordinal)

		}
		*/
	}

	@Override
	public void read(Row row, int ordinal) throws StandardException {
		/*
		if (row.isNullAt(ordinal))
			value = new SQLRowId();
		value.read(row,ordinal);
		*/
	}

	/**
	 *
	 * This calls the references encodedKeyLength method.
	 *
	 * @return
	 * @throws StandardException
     */
	@Override
	public int encodedKeyLength() throws StandardException {
		return -1;
//		return isNull()?1:value.encodedKeyLength(); // Order Does Not Matter for Length
	}

	/**
	 *
	 * This calls the references underlying encodeIntoKey
	 *
	 * @param src
	 * @param order
	 * @throws StandardException
     */
	@Override
	public void encodeIntoKey(PositionedByteRange src, Order order) throws StandardException {
		/*
		if (isNull())
				OrderedBytes.encodeNull(src, order);
		else
			value.encodeIntoKey(src,order);
			*/
	}

	/**
	 *
	 * This calls the references underlying decodeFromKey method.
	 *
	 * @param src
	 * @throws StandardException
     */
	@Override
	public void decodeFromKey(PositionedByteRange src) throws StandardException {
		/*
		if (OrderedBytes.isNull(src))
				setToNull();
		else
		if (value==null)
				value = new SQLRowId();
		value.decodeFromKey(src);
		*/
	}

	@Override
	public StructField getStructField(String columnName) {
		return DataTypes.createStructField(columnName, DataTypes.BinaryType, true);
	}

	public void updateThetaSketch(UpdateSketch updateSketch) {
		throw new UnsupportedOperationException("Not Implemented");
	}

}
