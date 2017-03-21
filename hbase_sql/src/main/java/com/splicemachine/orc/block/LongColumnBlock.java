package com.splicemachine.orc.block;

import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.LongType;

/**
 * Created by jleach on 3/17/17.
 */
public class LongColumnBlock extends AbstractColumnBlock {

    public LongColumnBlock(ColumnVector columnVector, DataType type) {
        super(columnVector,type);
    }

    @Override
    public Object getObject(int i) {
        return columnVector.isNullAt(i)?null:columnVector.getLong(i);
    }
}
