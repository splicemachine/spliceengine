package com.splicemachine.orc.block;

import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;

/**
 * Created by jleach on 3/17/17.
 */
public class MapColumnBlock extends AbstractColumnBlock {

    public MapColumnBlock(ColumnVector columnVector, DataType type) {
        super(columnVector,type);
    }

    @Override
    public Object getObject(int i) {
        return null;
    }

}
