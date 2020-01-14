/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package org.apache.spark.sql.execution.vectorized;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnarRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.sql.execution.datasources.parquet.ParquetDictionary;
import parquet.column.Dictionary;

public class ColumnVector {

    private WritableColumnVector delegate;

    private ColumnVector(int capacity, DataType type, MemoryMode mode) {
        delegate = mode == MemoryMode.ON_HEAP ? new OnHeapColumnVector(capacity, type) : new OffHeapColumnVector(capacity, type);
    }

    private ColumnVector(WritableColumnVector columnVector) {
        delegate = columnVector;
    }

    public static ColumnVector allocate(int size, DataType type, MemoryMode mode) {
        return new ColumnVector(size, type, mode);
    }

    WritableColumnVector getDelegate() {
        return delegate;
    }

    public void reset() {
        delegate.reset();
    }

    public void close() {
        delegate.close();
    }

    public void reserve(int requiredCapacity) {
        delegate.reserve(requiredCapacity);
    }

    public int getDictId(int rowId) {
        return delegate.getDictId(rowId);
    }

    protected void reserveInternal(int capacity) {
        delegate.reserveInternal(capacity);
    }

    public void putNotNull(int rowId) {
        delegate.putNotNull(rowId);
    }

    public void putNull(int rowId) {
        delegate.putNull(rowId);
    }

    public void putNulls(int rowId, int count) {
        delegate.putNulls(rowId, count);
    }

    public void putNotNulls(int rowId, int count) {
        delegate.putNotNulls(rowId, count);
    }

    public void putBoolean(int rowId, boolean value) {
        delegate.putBoolean(rowId, value);
    }

    public void putBooleans(int rowId, int count, boolean value) {
        delegate.putBooleans(rowId, count, value);
    }

    public void putByte(int rowId, byte value) {
        delegate.putByte(rowId, value);
    }

    public void putBytes(int rowId, int count, byte value) {
        delegate.putBytes(rowId, count, value);
    }

    public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
        delegate.putBytes(rowId, count, src, srcIndex);
    }

    public void putShort(int rowId, short value) {
        delegate.putShort(rowId, value);
    }

    public void putShorts(int rowId, int count, short value) {
        delegate.putShorts(rowId, count, value);
    }

    public void putShorts(int rowId, int count, short[] src, int srcIndex) {
        delegate.putShorts(rowId, count, src, srcIndex);
    }

    public void putShorts(int rowId, int count, byte[] src, int srcIndex) {
        delegate.putShorts(rowId, count, src, srcIndex);
    }

    public void putInt(int rowId, int value) {
        delegate.putInt(rowId, value);
    }

    public void putInts(int rowId, int count, int value) {
        delegate.putInts(rowId, count, value);
    }

    public void putInts(int rowId, int count, int[] src, int srcIndex) {
        delegate.putInts(rowId, count, src, srcIndex);
    }

    public void putInts(int rowId, int count, byte[] src, int srcIndex) {
        delegate.putInts(rowId, count, src, srcIndex);
    }

    public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
        delegate.putIntsLittleEndian(rowId, count, src, srcIndex);
    }

    public void putLong(int rowId, long value) {
        delegate.putLong(rowId, value);
    }

    public void putLongs(int rowId, int count, long value) {
        delegate.putLongs(rowId, count, value);
    }

    public void putLongs(int rowId, int count, long[] src, int srcIndex) {
        delegate.putLongs(rowId, count, src, srcIndex);
    }

    public void putLongs(int rowId, int count, byte[] src, int srcIndex) {
        delegate.putLongs(rowId, count, src, srcIndex);
    }

    public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
        delegate.putLongsLittleEndian(rowId, count, src, srcIndex);
    }

    public void putFloat(int rowId, float value) {
        delegate.putFloat(rowId, value);
    }

    public void putFloats(int rowId, int count, float value) {
        delegate.putFloats(rowId, count, value);
    }

    public void putFloats(int rowId, int count, float[] src, int srcIndex) {
        delegate.putFloats(rowId, count, src, srcIndex);
    }

    public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
        delegate.putFloats(rowId, count, src, srcIndex);
    }

    public void putDouble(int rowId, double value) {
        delegate.putDouble(rowId, value);
    }

    public void putDoubles(int rowId, int count, double value) {
        delegate.putDoubles(rowId, count, value);
    }

    public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
        delegate.putDoubles(rowId, count, src, srcIndex);
    }

    public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
        delegate.putDoubles(rowId, count, src, srcIndex);
    }

    public void putArray(int rowId, int offset, int length) {
        delegate.putArray(rowId, offset, length);
    }

    public int putByteArray(int rowId, byte[] value, int offset, int count) {
        return delegate.putByteArray(rowId, value, offset, count);
    }

    protected UTF8String getBytesAsUTF8String(int rowId, int count) {
        return delegate.getBytesAsUTF8String(rowId, count);
    }

    public int getArrayLength(int rowId) {
        return delegate.getArrayLength(rowId);
    }

    public int getArrayOffset(int rowId) {
        return delegate.getArrayOffset(rowId);
    }

    protected WritableColumnVector reserveNewColumn(int capacity, DataType type) {
        return delegate.reserveNewColumn(capacity, type);
    }

    public boolean isNullAt(int rowId) {
        return delegate.isNullAt(rowId);
    }

    public boolean getBoolean(int rowId) {
        return delegate.getBoolean(rowId);
    }

    public byte getByte(int rowId) {
        return delegate.getByte(rowId);
    }

    public short getShort(int rowId) {
        return delegate.getShort(rowId);
    }

    public int getInt(int rowId) {
        return delegate.getInt(rowId);
    }

    public long getLong(int rowId) {
        return delegate.getLong(rowId);
    }

    public float getFloat(int rowId) {
        return delegate.getFloat(rowId);
    }

    public double getDouble(int rowId) {
        return delegate.getDouble(rowId);
    }

    public byte[] getBinary(int rowId) {
        return delegate.getBinary(rowId);
    }

    public Decimal getDecimal(int ordinal, int precision, int scale) {
        return delegate.getDecimal(ordinal, precision, scale);
    }

    public void putDecimal(int rowId, Decimal value, int precision) {
        delegate.putDecimal(rowId, value, precision);
    }

    public UTF8String getUTF8String(int rowId) {
        return delegate.getUTF8String(rowId);
    }

    public ColumnVector getDictionaryIds() {
        return new ColumnVector(delegate.getDictionaryIds());
    }

    public WritableColumnVector reserveDictionaryIds(int capacity) {
        return delegate.reserveDictionaryIds(capacity);
    }

    public void setDictionary(Dictionary dictionary) {
        delegate.setDictionary(new ParquetDictionary(dictionary));
    }

    public final int appendNull() {
        return delegate.appendNull();
    }

    public final int appendNotNull() {
        return delegate.appendNotNull();
    }

    public final int appendNulls(int count) {
        return delegate.appendNulls(count);
    }

    public final int appendBoolean(boolean v) {
        return delegate.appendBoolean(v);
    }

    public final int appendBooleans(int count, boolean v) {
        return delegate.appendBooleans(count, v);
    }

    public final int appendByte(byte v) {
        return delegate.appendByte(v);
    }

    public final int appendBytes(int count, byte v) {
        return delegate.appendBytes(count, v);
    }

    public final int appendShort(short v) {
        return delegate.appendShort(v);
    }

    public final int appendShorts(int count, short v) {
        return delegate.appendShorts(count, v);
    }

    public final int appendInt(int v) {
        return delegate.appendInt(v);
    }

    public final int appendInts(int count, int v) {
        return delegate.appendInts(count, v);
    }

    public final int appendLong(long v) {
        return delegate.appendLong(v);
    }

    public final int appendLongs(int count, long v) {
        return delegate.appendLongs(count, v);
    }

    public final int appendFloat(float v) {
        return delegate.appendFloat(v);
    }

    public final int appendFloats(int count, float v) {
        return delegate.appendFloats(count, v);
    }

    public final int appendDouble(double v) {
        return delegate.appendDouble(v);
    }

    public final int appendDoubles(int count, double v) {
        return delegate.appendDoubles(count, v);
    }

    public final int appendByteArray(byte[] value, int offset, int length) {
        return delegate.appendByteArray(value, offset, length);
    }

    public final int appendStruct(boolean isNull) {
        return delegate.appendStruct(isNull);
    }

    public ColumnVector getChildColumn(int ordinal) {
        return new ColumnVector(delegate.getChild(ordinal));
    }

    public ColumnVector arrayData() {
        return new ColumnVector((delegate.arrayData()));
    }

    public final int getElementsAppended() {
        return delegate.getElementsAppended();
    }

    public static class StructStub {
        ColumnVector[] columns;

        public StructStub(ColumnVector[] columns) {
            this.columns = columns;
        }

        public ColumnVector[] columns() {
            return columns;
        }
    }

    public StructStub getStruct(int rowId) {
        ColumnarRow row = delegate.getStruct(rowId);
        int n = row.numFields();
        ColumnVector[] columns = new ColumnVector[n];
        for (int i = 0; i < n; ++i) {
            columns[i] = new ColumnVector(delegate.getChild(i));
        }
        return new StructStub(columns);
    }
}
