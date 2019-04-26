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

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLDecimal;
import com.splicemachine.db.iapi.types.TypeId;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Map;
import scala.collection.Seq;
import scala.util.hashing.MurmurHash3;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 Basic implementation of ExecRow.

 */
public class ValueRow implements ExecRow, Externalizable {
	///////////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	///////////////////////////////////////////////////////////////////////
	private DataValueDescriptor[] column;
	private int ncols;
	private int hash = 0; // Cached value of hashCode(). Invalidate on any change to the object.
	private byte[] key;

	///////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	///////////////////////////////////////////////////////////////////////

	/** Empty constructor for serialization */
	public ValueRow() {

	}

	/**
	 *
	 * Pass in column array
	 *
	 * @param column
	 */
	public ValueRow(DataValueDescriptor[] column) {
		this(column.length);
		this.column = column;
	}


	/**
	 *	Make a value row with a designated number of column slots.
	 *
	 *	@param	ncols	number of columns to allocate
	 */
	public ValueRow(int ncols)
	{
		column = new DataValueDescriptor[ncols];
		this.ncols = ncols;
	}


	///////////////////////////////////////////////////////////////////////
	//
	//	EXECROW INTERFACE
	//
	///////////////////////////////////////////////////////////////////////


	// this is the actual current # of columns
	public int nColumns() {
		return ncols;
	}

	// get a new Object[] for the row
	public void getNewObjectArray()
	{
		column = new DataValueDescriptor[ncols];
		hash = 0;
	}

	/*
	 * Row interface
	 */
	// position is 1-based
	public DataValueDescriptor	getColumn (int position) {
		try {
			return column[position-1];
		} catch (Exception e) {
			return (DataValueDescriptor)null;
		}
	}

	// position is 1-based.
	public void setColumn(int position, DataValueDescriptor col) {
		hash = 0;
		try {
			column[position-1] = col;
		} catch (Exception e) {
			realloc(position);
			column[position-1] = col;
		}
	}

	public void normalizeDecimal(DataValueDescriptor dvd,
                                 int desiredScale,
                                 int desiredPrecision) throws StandardException {
	    SQLDecimal decimal = (SQLDecimal) dvd;
	    decimal.setPrecision(desiredPrecision);
	    decimal.setScale(desiredScale);
	}

    // Same as setColumn, but preserves the data type of the dvd we're replacing.
    public void setColumnValue(int position, DataValueDescriptor col) throws StandardException {
        hash = 0;
        DataValueDescriptor dvd = column[position-1];
        int precision = 0, scale = 0;

        try {
            if (column[position-1] == null)
                column[position-1] = col;
            else {
                if (column[position - 1] instanceof SQLDecimal) {
                    SQLDecimal decimal = (SQLDecimal)column[position - 1];
                    precision = decimal.getPrecision();
                    scale = decimal.getScale();
                }
                column[position - 1].setValue(col);
                if (precision != 0)
                    normalizeDecimal(column[position - 1], scale, precision);
            }
        } catch (Exception e) {
            realloc(position);
            if (column[position-1] == null)
                column[position-1] = col;
            else {
                column[position - 1].setValue(col);
                if (precision != 0)
                    normalizeDecimal(column[position - 1], scale, precision);
            }
        }
    }

	/*
	** ExecRow interface
	*/

	// position is 1-based
	public ExecRow getClone()
	{
		return getClone((FormatableBitSet) null);
	}

	public ExecRow getClone(FormatableBitSet clonedCols)
	{
		int numColumns = column.length;

		/* Get the right type of row */
		ValueRow rowClone = cloneMe();

		for (int colCtr = 0; colCtr < numColumns; colCtr++) {
			// Copy those columns whose bit isn't set (and there is a FormatableBitSet)
			if (clonedCols != null && !(clonedCols.get(colCtr + 1)))
			{
				/* Rows are 1-based, column[] is 0-based */
				rowClone.setColumn(colCtr + 1, (DataValueDescriptor) column[colCtr]);
				continue;
			}

			if (column[colCtr] != null)
			{
				/* Rows are 1-based, column[] is 0-based */
				rowClone.setColumn(colCtr +1, column[colCtr].cloneValue(false));
			}
		}
		rowClone.setKey(getKey());
		return rowClone;
	}

	public void transfer(ExecRow execRow) throws StandardException {
		for (int i = 0; i< ncols; i++) {
			column[i].setValue(execRow.getColumn(i+1));
		}
	}

	// position is 1-based
	public ExecRow getNewNullRow()
	{
		int numColumns = column.length;
		ValueRow rowClone = cloneMe();


		for (int colCtr = 0; colCtr < numColumns; colCtr++)
		{
			if (column[colCtr] != null)
			{
				/* Rows are 1-based, column[] is 0-based */
				rowClone.setColumn(colCtr + 1, column[colCtr].getNewNull());
			}
		}
		return rowClone;
	}

	public ValueRow cloneMe() {
		return new ValueRow(ncols);
	}

	/**
	 * Reset all columns in the row array to null values.
	 */
	public void resetRowArray() {
		hash = 0;
		for (int i = 0; i < column.length; i++) {
			if (column[i] != null) {
				column[i] = column[i].recycle();
			}
		}
	}

	// position is 1-based
	public final DataValueDescriptor cloneColumn(int columnPosition)
	{
		return column[columnPosition -1].cloneValue(false);
	}

	/*
	 * class interface
	 */
	public String toString() {
		// NOTE: This method is required for external functionality (the
		// consistency checker), so do not put it under SanityManager.DEBUG.
		StringBuilder s = new StringBuilder("{ ");
		for (int i = 0; i < column.length; i++)
		{
			if (column[i] == null)
				s.append("null");
			else
				s.append(column[i].toString());
			if (i < (column.length - 1))
				s.append(", ");
		}
		s.append(" }");
		return s.toString();
	}


	/**
	 Get the array form of the row that Access expects.

	 @see ExecRow#getRowArray
	 */
	public DataValueDescriptor[] getRowArray() {
		return column;
	}

	/**
	 Get a clone of the array form of the row that Access expects.

	 @see ExecRow#getRowArray
	 */
	public DataValueDescriptor[] getRowArrayClone()
	{
		int numColumns = column.length;
		DataValueDescriptor[] columnClones = new DataValueDescriptor[numColumns];

		for (int colCtr = 0; colCtr < numColumns; colCtr++)
		{
			if (column[colCtr] != null)
			{
				columnClones[colCtr] = column[colCtr].cloneValue(false);
			}
		}

		return columnClones;
	}

	/**
	 * Set the row array
	 *
	 * @see ExecRow#setRowArray
	 */
	public void setRowArray(DataValueDescriptor[] value)
	{
		hash = 0;
		column = value;
	}

	// Set the number of columns in the row to ncols, preserving
	// the existing contents.
	protected void realloc(int ncols) {
		DataValueDescriptor[] newcol = new DataValueDescriptor[ncols];

		System.arraycopy(column, 0, newcol, 0, column.length);
		column = newcol;

	}

	@Override
	public int compareTo(ExecRow row) {
		if (row == null)
			return -1;
		if (ncols != row.nColumns())
			return -1;
		int compare;
		for (int i = 1; i <= ncols; i++ ) {
			try {
				compare = getColumn(i).compare(row.getColumn(i));
				if (compare != 0)
					return compare;
			} catch (StandardException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return 0;
	}

	@Override
	public int compareTo(int[] compareKeys, ExecRow row) {
		if (row == null)
			return -1;
		if (ncols != row.nColumns())
			return -1;
		int compare;
		for (int compareKey : compareKeys) {
			try {
				compare = getColumn(compareKey).compare(row.getColumn(compareKey));
				if (compare != 0)
					return compare;
			} catch (StandardException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return 0;
	}


	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(hash);
		out.writeInt(ncols);
		for (DataValueDescriptor desc: column) {
			out.writeObject(desc);
		}
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		hash = in.readInt();
		ncols = in.readInt();
		column = new DataValueDescriptor[ncols];
		for (int i = 0; i < ncols; i++) {
			column[i] = (DataValueDescriptor) in.readObject();
		}
	}

	public int hashCode() {
		if (hash == 0) {
			if (column.length > 0)
			    hash = MurmurHash3.arrayHashing().hash(column);
			else
				hash = 123456789;
		}
		return hash;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof ValueRow))
			return false;
		ValueRow other = (ValueRow) obj;
		return Arrays.equals(column, other.column) && ncols == other.ncols;
	}

	@Override
	public ExecRow getKeyedExecRow(int[] keyColumns) throws StandardException {
		ValueRow key = new ValueRow(keyColumns.length);
		int position = 1;
		for (int keyColumn : keyColumns) {
			key.setColumn(position++, getColumn(keyColumn + 1));
		}
		return key;
	}

	@Override
	public int hashCode(int[] keysToHash) {
		final int prime = 31;
		int result = 1;
		if (column == null)
			return 0;
		for (int hashKey: keysToHash) {
			result = 31 * result + (column[hashKey] == null ? 0 : column[hashKey].hashCode());
		}
		result = prime * result + keysToHash.length;
		return result;
	}

	public org.apache.spark.sql.Row getSparkRow() {
		return this;
	}

	public ExecRow fromSparkRow(org.apache.spark.sql.Row row) {
		try {
			int size = row.length() < ncols?row.length():ncols; // Fix for antijoin
			for (int i = 0; i < size; i++) {
				column[i].read(row,i);
			}
			return this;
		}
		catch (StandardException se) {
			throw new RuntimeException(se);
		}
	}

	@Override
	public int size() {
		return ncols;
	}

	@Override
	public int length() {
		return ncols;
	}

	@Override
	public StructType schema() {
		StructField[] fields = new StructField[ncols];
		for (int i = 0; i < ncols;i++)
			fields[i] = column[i].getStructField(getNamedColumn(i));
		return DataTypes.createStructType(fields);
	}

	@Override
	public Object apply(int i) {
		try {
			return column[i].getObject();
		} catch (StandardException se) {
			throw new RuntimeException(se);
		}
	}

	@Override
	public Object get(int i) {
		try {
			return column[i].getSparkObject();
		} catch (StandardException se) {
			throw new RuntimeException(se);
		}
	}

	@Override
	public boolean isNullAt(int i) {
		return column[i].isNull();
	}

	@Override
	public boolean getBoolean(int i) {
		try {
			return column[i].getBoolean();
		} catch (StandardException se) {
			throw new RuntimeException(se);
		}
	}

	@Override
	public byte getByte(int i) {
		try {
			return column[i].getByte();
		} catch (StandardException se) {
			throw new RuntimeException(se);
		}
	}

	@Override
	public short getShort(int i) {
		try {
			return column[i].getShort();
		} catch (StandardException se) {
			throw new RuntimeException(se);
		}

	}

	@Override
	public int getInt(int i) {
		try {
			return column[i].getInt();
		} catch (StandardException se) {
			throw new RuntimeException(se);
		}
	}

	@Override
	public long getLong(int i) {
		try {
			return column[i].getLong();
		} catch (StandardException se) {
			throw new RuntimeException(se);
		}
	}

	@Override
	public float getFloat(int i) {
		try {
			return column[i].getFloat();
		} catch (StandardException se) {
			throw new RuntimeException(se);
		}
	}

	@Override
	public double getDouble(int i) {
		try {
			return column[i].getDouble();
		} catch (StandardException se) {
			throw new RuntimeException(se);
		}
	}

	@Override
	public String getString(int i) {
		try {
			return column[i].getString();
		} catch (StandardException se) {
			throw new RuntimeException(se);
		}
	}

	@Override
	public BigDecimal getDecimal(int i) {
		try {
			return (BigDecimal) column[i].getObject();
		} catch (StandardException se) {
			throw new RuntimeException(se);
		}
	}

	@Override
	public Date getDate(int i) {
		try {
			return column[i].getDate(null);
		} catch (StandardException se) {
			throw new RuntimeException(se);
		}
	}

	@Override
	public Timestamp getTimestamp(int i) {
		try {
			return column[i].getTimestamp(null);
		} catch (StandardException se) {
			throw new RuntimeException(se);
		}
	}

	@Override
	public <T> Seq<T> getSeq(int i) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> List<T> getList(int i) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <K, V> Map<K, V> getMap(int i) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <K, V> java.util.Map<K, V> getJavaMap(int i) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Row getStruct(int i) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T getAs(int i) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T getAs(String s) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int fieldIndex(String s) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> scala.collection.immutable.Map<String, T> getValuesMap(Seq<String> seq) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Row copy() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean anyNull() {
		return false;
	}

	@Override
	public Seq<Object> toSeq() {
		return scala.collection.JavaConversions.asScalaBuffer(Arrays.asList(column));
	}

	@Override
	public String mkString() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String mkString(String s) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String mkString(String s, String s1, String s2) {
		throw new UnsupportedOperationException();
	}

	@Override
	public StructType createStructType(int[] baseColumnMap) {
		// use baseColumnMap to ensure all selected columns (at each index i) are returned
		StructField[] fields = new StructField[length()];
		int j = 0;
		for(int i = 0; i < baseColumnMap.length; i++){
			if (baseColumnMap[i] != -1){
				assert j < fields.length : "Incorrect number of columns";
				fields[j] = getColumn(j+1).getStructField(getNamedColumn(i));
				j++;
			}
		}
		return DataTypes.createStructType(fields);
	}

	@Override
	public int compare(ExecRow o1, ExecRow o2) {
		return o1.compareTo(o2);
	}

	@Override
	public long getRowSize() throws StandardException {
		long rowSize = 0L;
		for (DataValueDescriptor dvd: column) {
			int length = dvd.getLength();

			rowSize += length >= 0?length:dvd.estimateMemoryUsage();
		}
		return rowSize;
	}

	@Override
	public long getRowSize(BitSet validColumns) throws StandardException {
		if (validColumns ==null)
			return getRowSize();
		long rowSize = 0L;
		int nextSetBit = 0;
		while ( (nextSetBit = validColumns.nextSetBit(nextSetBit)) != -1)
			rowSize += (long) column[nextSetBit].getLength();
		return rowSize;
	}

	public static String getNamedColumn(int columnNumber) {
		return "c"+columnNumber;
	}

	@Override
	public byte[] getKey() {
		return key;
	}

	@Override
	public void setKey(byte[] key) {
		this.key = key;
	}
}
