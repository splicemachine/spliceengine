package com.splicemachine.orc.block;

import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType;

/**
 * Created by jleach on 3/17/17.
 */
public class IntegerColumnBlock extends AbstractColumnBlock {

    public IntegerColumnBlock(ColumnVector columnVector, DataType type) {
        super(columnVector,type);
    }

    @Override
    public Object getObject(int i) {
        return columnVector.isNullAt(i)?null:columnVector.getInt(i);
    }
}
