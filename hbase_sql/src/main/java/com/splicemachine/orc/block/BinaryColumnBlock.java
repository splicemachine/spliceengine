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

import com.splicemachine.primitives.Bytes;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;

/**
 * Created by jleach on 3/17/17.
 */
public class BinaryColumnBlock extends AbstractColumnBlock {

    public BinaryColumnBlock(ColumnVector columnVector, DataType type) {
        super(columnVector,type);
    }

    @Override
    public Object getObject(int i) {
        return columnVector.isNullAt(i)?null:columnVector.getBinary(i);
    }

    @Override
    public Object getTestObject(int i) {
        try {
            Object object = getObject(i);
            return object != null ? Bytes.toString( (byte[]) object):null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setPartitionValue(String value, int size) {
        throw new UnsupportedOperationException("Not Supported");
    }

}
