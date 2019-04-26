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
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import scala.collection.Map;
import scala.collection.Seq;
import java.io.*;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.BitSet;
import java.util.List;

/**
	Mapper of ValueRow into ExecIndexRow. 

 */
public class IndexValueRow implements ExecIndexRow, Serializable {

	private ExecRow valueRow;

	public IndexValueRow(ExecRow valueRow) {
		 this.valueRow = valueRow;
	}

	@Override
	public int size() {
		return valueRow.size();
	}

	@Override
	public int length() {
		return valueRow.length();
	}

	@Override
	public StructType schema() {
		return valueRow.schema();
	}

	@Override
	public Object apply(int i) {
		return valueRow.apply(i);
	}

	@Override
	public Object get(int i) {
		return valueRow.get(i);
	}

	@Override
	public boolean isNullAt(int i) {
		return valueRow.isNullAt(i);
	}

	@Override
	public boolean getBoolean(int i) {
		return valueRow.getBoolean(i);
	}

	@Override
	public byte getByte(int i) {
		return valueRow.getByte(i);
	}

	@Override
	public short getShort(int i) {
		return valueRow.getShort(i);
	}

	@Override
	public int getInt(int i) {
		return valueRow.getInt(i);
	}

	@Override
	public long getLong(int i) {
		return valueRow.getLong(i);
	}

	@Override
	public float getFloat(int i) {
		return valueRow.getFloat(i);
	}

	@Override
	public double getDouble(int i) {
		return valueRow.getDouble(i);
	}

	@Override
	public String getString(int i) {
		return valueRow.getString(i);
	}

	@Override
	public BigDecimal getDecimal(int i) {
		return valueRow.getDecimal(i);
	}

	@Override
	public Date getDate(int i) {
		return valueRow.getDate(i);
	}

	@Override
	public Timestamp getTimestamp(int i) {
		return valueRow.getTimestamp(i);
	}

	@Override
	public <T> Seq<T> getSeq(int i) {
		return valueRow.getSeq(i);
	}

	@Override
	public <T> List<T> getList(int i) {
		return valueRow.getList(i);
	}

	@Override
	public <K, V> Map<K, V> getMap(int i) {
		return valueRow.getMap(i);
	}

	@Override
	public <K, V> java.util.Map<K, V> getJavaMap(int i) {
		return valueRow.getJavaMap(i);
	}

	@Override
	public Row getStruct(int i) {
		return valueRow.getStruct(i);
	}

	@Override
	public <T> T getAs(int i) {
		return valueRow.getAs(i);
	}

	@Override
	public <T> T getAs(String s) {
		return valueRow.getAs(s);
	}

	@Override
	public int fieldIndex(String s) {
		return valueRow.fieldIndex(s);
	}

	@Override
	public <T> scala.collection.immutable.Map<String, T> getValuesMap(Seq<String> seq) {
		return valueRow.getValuesMap(seq);
	}

	/*
         * class interface
         */
	public String toString() {
		return valueRow.toString();
	}

	@Override
	public Row copy() {
		return null;
	}

	@Override
	public boolean anyNull() {
		return false;
	}


	/**
		Get the array form of the row that Access expects.

		@see ExecRow#getRowArray
	*/
	public DataValueDescriptor[] getRowArray() {
		return valueRow.getRowArray();
	}

	/**	@see ExecRow#getRowArray */
	public void setRowArray(DataValueDescriptor[] value) 
	{
		valueRow.setRowArray(value);
	}

	/**
		Get a clone of the array form of the row that Access expects.

		@see ExecRow#getRowArray
	*/
	public DataValueDescriptor[] getRowArrayClone() 
	{
		return valueRow.getRowArrayClone();
	}

	// this is the actual current # of columns
	public int nColumns() {
		return valueRow.nColumns();
	}

	/*
	 * Row interface
	 */
	// position is 1-based
	public DataValueDescriptor	getColumn (int position) throws StandardException {
		return valueRow.getColumn(position);
	}

	// position is 1-based.
	public void setColumn(int position, DataValueDescriptor col) {
		valueRow.setColumn(position, col);
	}

	// position is 1-based.
	public void setColumnValue(int position, DataValueDescriptor col) throws StandardException {
		valueRow.setColumnValue(position, col);
	}

	// position is 1-based
	public ExecRow getClone() {
		return new IndexValueRow(valueRow.getClone());
	}

	public ExecRow getClone(FormatableBitSet clonedCols) {
		return new IndexValueRow(valueRow.getClone(clonedCols));
	}

	public ExecRow getNewNullRow() {
		return new IndexValueRow(valueRow.getNewNullRow());
	}

    /**
     * Reset all columns in the row array to null values.
     */
    public void resetRowArray() {
        valueRow.resetRowArray();
    }

	// position is 1-based
	public DataValueDescriptor cloneColumn(int columnPosition)
	{
		return valueRow.cloneColumn(columnPosition);
	}

	/*
	 * ExecIndexRow interface
	 */

	public void orderedNulls(int columnPosition) {
		if (SanityManager.DEBUG) {
			SanityManager.THROWASSERT("Not expected to be called");
		}
	}

	public boolean areNullsOrdered(int columnPosition) {
		if (SanityManager.DEBUG) {
			SanityManager.THROWASSERT("Not expected to be called");
		}

		return false;
	}

	/**
	 * Turn the ExecRow into an ExecIndexRow.
	 */
	public void execRowToExecIndexRow(ExecRow valueRow)
	{
		this.valueRow = valueRow;
	}

	public void getNewObjectArray() 
	{
		valueRow.getNewObjectArray();
	}

    @Override
    public int hashCode() {
        return valueRow.hashCode();
    }

	@Override
	public Seq<Object> toSeq() {
		return null;
	}

	@Override
	public String mkString() {
		return null;
	}

	@Override
	public String mkString(String s) {
		return null;
	}

	@Override
	public String mkString(String s, String s1, String s2) {
		return null;
	}

	@Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexValueRow that = (IndexValueRow) o;

		return valueRow != null ? valueRow.equals(that.valueRow) : that.valueRow == null;

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
        try {
            final int prime = 31;
            int result = 1;
            if (getRowArray() == null)
                return 0;
            for (int hashKey : keysToHash) {
                result = 31 * result + (getColumn(hashKey) == null ? 0 : getColumn(hashKey).hashCode());
            }
            result = prime * result + keysToHash.length;
            return result;
        } catch (StandardException se) {
            throw new RuntimeException(se);
        }
    }

    @Override
    public int compareTo(int[] compareKeys, ExecRow row) {
        return valueRow.compareTo(compareKeys,row);
    }

	@Override
	public StructType createStructType(int[] baseColumnMap) {
		return valueRow.createStructType(baseColumnMap);
	}

	@Override
	public Row getSparkRow() {
		return valueRow.getSparkRow();
	}

	@Override
	public ExecRow fromSparkRow(Row row) {
		return valueRow.fromSparkRow(row);
	}

	public int compareTo(ExecRow o) {
		return valueRow.compareTo(o);
	}

	@Override
	public int compare(ExecRow o1, ExecRow o2) {
		return o1.compareTo(o2);
	}

	@Override
	public long getRowSize() throws StandardException {
		return valueRow.getRowSize();
	}

	@Override
	public long getRowSize(BitSet validColumns) throws StandardException {
		return valueRow.getRowSize(validColumns);
	}

	@Override
	public byte[] getKey() {
		return valueRow.getKey();
	}

	@Override
	public void setKey(byte[] key) {
		valueRow.setKey(key);
	}

	@Override
	public void transfer(ExecRow execRow) throws StandardException {
		valueRow.transfer(execRow);
	}
}
