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

import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;

/**
 *
 *
 *
 */
public class LazyColumnBlock implements ColumnBlock {
    LazyColumnBlockLoader blockLoader;

    public LazyColumnBlock(LazyColumnBlockLoader blockLoader) {
        this.blockLoader = blockLoader;
    }

    @Override
    public ColumnVector getColumnVector() {
        return blockLoader.getColumnBlock().getColumnVector();
    }

    @Override
    public void setColumnVector(ColumnVector columnVector) {
        blockLoader.getColumnBlock().setColumnVector(columnVector);
    }

    @Override
    public DataType getDataType() {
        return blockLoader.getColumnBlock().getDataType();
    }

    @Override
    public void setDataType(DataType dataType) {
        blockLoader.getColumnBlock().setDataType(dataType);
    }

    @Override
    public Object getObject(int i) {
        return blockLoader.getColumnBlock().getObject(i);
    }

    @Override
    public Object getTestObject(int i) {
        return blockLoader.getColumnBlock().getTestObject(i);
    }

    @Override
    public Object[] getTestObjectArray(int offset, int length) {
        return blockLoader.getColumnBlock().getTestObjectArray(offset,length);
    }

    @Override
    public void setPartitionValue(String value, int size) {
        blockLoader.getColumnBlock().setPartitionValue(value,size);
    }

    @Override
    public void setPartitionNull(int size) {
        blockLoader.getColumnBlock().setPartitionNull(size);
    }
}
