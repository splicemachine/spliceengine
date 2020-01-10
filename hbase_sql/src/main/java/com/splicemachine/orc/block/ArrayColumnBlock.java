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
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import java.util.Arrays;

/**
 *
 *
 *
 */
public class ArrayColumnBlock extends AbstractColumnBlock {
    ArrayType arrayType;
    ColumnBlock columnBlock;
    public ArrayColumnBlock(ColumnVector columnVector, DataType type) {
        super(columnVector,type);
        arrayType = (ArrayType) type;
        columnBlock = BlockFactory.getColumnBlock(columnVector.getChildColumn(0),arrayType.elementType());
    }

    @Override
    public Object getObject(int i) {
        if (columnVector.isNullAt(i))
            return null;
        return Arrays.asList(columnBlock.getTestObjectArray(columnVector.getArrayOffset(i),columnVector.getArrayLength(i)));
    }

    @Override
    public void setPartitionValue(String value, int size) {
        throw new UnsupportedOperationException("Operation Not Supported");
    }

    @Override
    public void setPartitionNull(int size) {
        columnVector = ColumnVector.allocate(size, dataType, MemoryMode.ON_HEAP);
        columnVector.appendNulls(size);
    }

}
