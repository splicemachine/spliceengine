package com.splicemachine.orc.block;

import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;

/**
 * Created by jleach on 3/17/17.
 */
public class DecimalColumnBlock extends AbstractColumnBlock {
    DecimalType decimalType;
    public DecimalColumnBlock(ColumnVector columnVector, DataType type) {
        super(columnVector,type);
        decimalType = (DecimalType) type;
    }

    @Override
    public Object getObject(int i) {
        return columnVector.isNullAt(i)?null:columnVector.getDecimal(i, decimalType.precision(), decimalType.scale());
    }
}
