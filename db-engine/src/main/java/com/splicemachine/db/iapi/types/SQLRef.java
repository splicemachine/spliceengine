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
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.iapi.services.cache.ClassSize;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.RowId;
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

public class SQLRef extends DataType implements RefDataValue {
	protected RowLocation	value;

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( SQLRef.class);

    public int estimateMemoryUsage()
    {
        int sz = BASE_MEMORY_USAGE;
        if( null != value)
            sz += value.estimateMemoryUsage();
        return sz;
    } // end of estimateMemoryUsage

	/*
	** DataValueDescriptor interface
	** (mostly implemented in DataType)
	*/

	public String getString()
	{
		if (value != null)
		{
			return value.toString();
		}
		else
		{
			return null;
		}
	}

	public Object getObject()
	{
		return value;
	}

	protected void setFrom(DataValueDescriptor theValue) throws StandardException {

		if (theValue.isNull())
			setToNull();
		else
			setValue((RowLocation) theValue.getObject());
	}

	public int getLength()
	{
		return TypeDescriptor.MAXIMUM_WIDTH_UNKNOWN;
	}

	/* this is for DataType's error generator */
	public String getTypeName()
	{
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
		return StoredFormatIds.SQL_REF_ID;
	}  

	private final boolean evaluateNull()
	{
		return (value == null);
	}

	public void writeExternal(ObjectOutput out) throws IOException {

        out.writeBoolean(value != null);
        if (value != null) {
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
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
	{
        boolean nonNull = in.readBoolean();
        if (nonNull) {
            setValue((RowLocation) in.readObject());
        }
	}
	public void readExternalFromArray(ArrayInputStream in) throws IOException, ClassNotFoundException
	{
		setValue((RowLocation) in.readObject());
	}

	/**
	 * @see com.splicemachine.db.iapi.services.io.Storable#restoreToNull
	 */

	public void restoreToNull()
	{
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
					throws StandardException
	{
		return value.compare(op,
							((SQLRef) other).value,
							orderedNulls,
							unknownRV);
	}

	/** @exception StandardException	Thrown on error */
	public int compare(DataValueDescriptor other) throws StandardException
	{
        if (other instanceof SQLRef) {
            return value.compare(((SQLRef) other).value);
        } else if (other instanceof StringDataValue) {
            return value.toString().compareToIgnoreCase(other.getString());
        }
        else {
            throw StandardException.newException("cannot compare SQLRef with " + other.getClass());
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
		if (value == null)
			return new SQLRef();
		else
           return new SQLRef((RowLocation) value.cloneValue(false));
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLRef();
	}

	/** 
	 * @see DataValueDescriptor#setValueFromResultSet 
	 *
	 */
	public void setValueFromResultSet(ResultSet resultSet, int colNumber,
									  boolean isNullable)
	{
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT(
				"setValueFromResultSet() is not supposed to be called for SQLRef.");
	}
	public void setInto(PreparedStatement ps, int position)  {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT(
				"setValueInto(PreparedStatement) is not supposed to be called for SQLRef.");
	}

	/*
	** Class interface
	*/

	/*
	** Constructors
	*/

	public SQLRef()
	{
	}

	public SQLRef(RowLocation rowLocation)
	{
		setValue(rowLocation);
	}

    @Override
	public void setValue(RowLocation rowLocation)
	{
		value = rowLocation;
		isNull = evaluateNull();
	}

    public void setValue(RowId rowId) {
		value = (SQLRowId)rowId;
		isNull = evaluateNull();
    }
	/*
	** String display of value
	*/

	public String toString()
	{
		if (value == null)
			return "NULL";
		else
			return value.toString();
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
		if (isNull())
				unsafeRowWriter.setNullAt(ordinal);
		else {
				value.write(unsafeRowWriter,ordinal);
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
		if (value==null)
				value = new SQLRowId();
		value.read(unsafeRow,ordinal);
	}

	@Override
	public void read(Row row, int ordinal) throws StandardException {
		if (row.isNullAt(ordinal))
			value = new SQLRowId();
		value.read(row,ordinal);
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
		return isNull()?1:value.encodedKeyLength(); // Order Does Not Matter for Length
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
		if (isNull())
				OrderedBytes.encodeNull(src, order);
		else
			value.encodeIntoKey(src,order);
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
		if (OrderedBytes.isNull(src))
				setToNull();
		else
		if (value==null)
				value = new SQLRowId();
		value.decodeFromKey(src);
	}

	@Override
	public StructField getStructField(String columnName) {
		return DataTypes.createStructField(columnName, DataTypes.BinaryType, true);
	}

	public void updateThetaSketch(UpdateSketch updateSketch) {
		value.updateThetaSketch(updateSketch);
	}

}
