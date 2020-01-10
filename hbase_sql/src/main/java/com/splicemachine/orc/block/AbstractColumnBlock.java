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
package com.splicemachine.orc.block;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;

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



}
