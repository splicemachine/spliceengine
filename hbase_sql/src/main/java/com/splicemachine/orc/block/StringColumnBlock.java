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

import com.splicemachine.orc.reader.SliceDictionary;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 *
 *
 *
 */
public class StringColumnBlock extends AbstractColumnBlock {

    public StringColumnBlock(ColumnVector columnVector, DataType type) {
        super(columnVector,type);
    }

    @Override
    public Object getObject(int i) {
        return columnVector.isNullAt(i)?null:columnVector.getUTF8String(i);
    }

    @Override
    public Object getTestObject(int i) {
        Object object = super.getTestObject(i);
        return object == null?null:object.toString();
    }

    @Override
    public void setPartitionValue(String value, int size) {
        try {
            columnVector = ColumnVector.allocate(size, DataTypes.IntegerType, MemoryMode.ON_HEAP);
            SliceDictionary dictionary = new SliceDictionary(new Slice[]{Slices.wrappedBuffer(value.getBytes("UTF-8"))});
            columnVector.setDictionary(dictionary);
            columnVector.reserveDictionaryIds(size);
            columnVector.getDictionaryIds().appendInts(size,0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
