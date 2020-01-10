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
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.ArrayList;

/**
 *
 *
 */
public class StructColumnBlock extends AbstractColumnBlock{
    StructType structType;
    DataType[] dataTypes;
    public StructColumnBlock(ColumnVector columnVector, DataType type) {
        super(columnVector,type);
        this.structType = (StructType) type;
        StructField[] fields = structType.fields();
        dataTypes = new DataType[fields.length];
        for (int i = 0; i< fields.length; i++) {
            dataTypes[i] = fields[i].dataType();
        }
    }

    @Override
    public Object getObject(int i) {
        if (columnVector.isNullAt(i))
            return null;
        ColumnVector[] columnVectors = columnVector.getStruct(i).columns();
        ArrayList struct = new ArrayList(columnVectors.length);
        int j = 0;
        for (ColumnVector child: columnVectors) {
            struct.add(BlockFactory.getColumnBlock(child,dataTypes[j]).getTestObject(i));
            j++;
        }
        return struct;
    }

    @Override
    public void setPartitionValue(String value, int size) {
        throw new UnsupportedOperationException("Not Supported");
    }
}
