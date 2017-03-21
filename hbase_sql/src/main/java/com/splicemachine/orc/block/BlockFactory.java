package com.splicemachine.orc.block;

import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.*;



/**
 * Created by jleach on 3/17/17.
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
