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
import org.apache.spark.sql.types.*;



/**
 *
 *
 */
public class BlockFactory {
    public static ColumnBlock getColumnBlock(ColumnVector columnVector, DataType dataType) {

        if (dataType instanceof ArrayType)
            return new ArrayColumnBlock(columnVector,dataType);
        else if (dataType instanceof BinaryType)
            return new BinaryColumnBlock(columnVector,dataType);
        else if (dataType instanceof BooleanType)
            return new BooleanColumnBlock(columnVector,dataType);
        else if (dataType instanceof ByteType)
            return new ByteColumnBlock(columnVector,dataType);
        else if (dataType instanceof DateType)
            return new DateColumnBlock(columnVector,dataType);
        else if (dataType instanceof DecimalType)
            return new DecimalColumnBlock(columnVector,dataType);
        else if (dataType instanceof DoubleType)
            return new DoubleColumnBlock(columnVector,dataType);
        else if (dataType instanceof FloatType)
            return new FloatColumnBlock(columnVector,dataType);
        else if (dataType instanceof IntegerType)
            return new IntegerColumnBlock(columnVector,dataType);
        else if (dataType instanceof LongType)
            return new LongColumnBlock(columnVector,dataType);
        else if (dataType instanceof MapType)
            return new MapColumnBlock(columnVector,dataType);
        else if (dataType instanceof ShortType)
            return new ShortColumnBlock(columnVector,dataType);
        else if (dataType instanceof StringType)
            return new StringColumnBlock(columnVector,dataType);
        else if (dataType instanceof StructType)
            return new StructColumnBlock(columnVector,dataType);
        else if (dataType instanceof TimestampType)
            return new TimestampColumnBlock(columnVector,dataType);
        else
            throw new UnsupportedOperationException("Type " + dataType);
    }


}
