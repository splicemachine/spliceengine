/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
package com.splicemachine.orc.block;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

/**
 *
 * Base Implementation of Column Block
 *
 *
 */
public abstract class AbstractColumnBlock implements ColumnBlock {
    protected ColumnVector columnVector;
    protected DataType dataType;

    public AbstractColumnBlock(DataType dataType) {
        this.dataType = dataType;
    }

    public AbstractColumnBlock(ColumnVector columnVector,DataType dataType) {
        this.columnVector = columnVector;
        this.dataType = dataType;
    }

    @Override
    public ColumnVector getColumnVector() {
        return this.columnVector;
    }

    @Override
    public void setColumnVector(ColumnVector columnVector) {
        this.columnVector = columnVector;
    }

    @Override
    public DataType getDataType() {
        return this.dataType;
    }

    @Override
    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public Object getTestObject(int i) {
        return getObject(i);
    }

    public Object[] getTestObjectArray(int offset, int length) {
        Object[] objects = new Object[length];
        for (int i = 0; i< length; i++)
            objects[i] = getTestObject(offset+i);
        return objects;
    }

    @Override
    public void setPartitionNull(int size) {
        columnVector = ColumnVector.allocate(size, dataType, MemoryMode.ON_HEAP);
        columnVector.appendNulls(size);
    }

    @Override
    public int getPositionCount() {
        return this.columnVector.getElementsAppended();
    }

    @Override
    public boolean getBoolean(int position) {
        return this.columnVector.getBoolean(position);
    }

    @Override
    public boolean isNull(int position) {
        return this.columnVector.isNullAt(position);
    }

    @Override
    public long getLong(int position) {
        return this.columnVector.getLong(position);
    }

    @Override
    public byte getByte(int position) {
        return this.columnVector.getByte(position);
    }
    @Override
    public double getDouble(int position) {
        return this.columnVector.getDouble(position);
    }
    @Override
    public float getFloat(int position) {
        return this.columnVector.getFloat(position);
    }

    @Override
    public Decimal getDecimal(int positon, int precision, int scale) {
        return this.columnVector.getDecimal(positon,precision,scale);
    }

    @Override
    public UTF8String getUTF8String(int position) {
        return this.columnVector.getUTF8String(position);
    }

    @Override
    public byte[] getBinary(int position) {
        return this.columnVector.getBinary(position);
    }

    @Override
    public void setNull() {
        columnVector.appendNull();
    }

    @Override
    public void setValue(DataValueDescriptor dvd) throws StandardException {
        throw new UnsupportedOperationException("Not Supported");
    }
}
