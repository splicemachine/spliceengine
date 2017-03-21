package com.splicemachine.orc.block;

import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StringType;

/**
 * Created by jleach on 3/17/17.
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
}
