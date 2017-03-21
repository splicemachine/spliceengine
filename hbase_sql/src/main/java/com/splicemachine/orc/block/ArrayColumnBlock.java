package com.splicemachine.orc.block;

import com.google.common.collect.Lists;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;

import java.util.Arrays;

/**
 * Created by jleach on 3/17/17.
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

}
