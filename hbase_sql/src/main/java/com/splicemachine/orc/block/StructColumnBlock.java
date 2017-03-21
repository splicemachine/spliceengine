package com.splicemachine.orc.block;

import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;

/**
 * Created by jleach on 3/17/17.
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
        ArrayList struct = new ArrayList();
        int j = 0;
        for (ColumnVector child: columnVectors) {
            struct.add(BlockFactory.getColumnBlock(child,dataTypes[j]).getTestObject(i));
            j++;
        }
        return struct;
    }
}
