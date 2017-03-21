package com.splicemachine.orc.block;

import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;

/**
 * Created by jleach on 3/17/17.
 */
public abstract class AbstractColumnBlock implements ColumnBlock {
    protected ColumnVector columnVector;
    protected DataType dataType;


    public AbstractColumnBlock(ColumnVector columnVector,DataType dataType) {
        this.columnVector = columnVector;
        this.dataType = dataType;
    }

    @Override
    public ColumnVector getColumnVector() {
        return this.columnVector;
    }

    @Override
    public void setColumnVector(ColumnVector columnVector) {
        this.columnVector = columnVector;
    }

    @Override
    public DataType getDataType() {
        return this.dataType;
    }

    @Override
    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public Object getTestObject(int i) {
        return getObject(i);
    }

    public Object[] getTestObjectArray(int offset, int length) {
        Object[] objects = new Object[length];
        for (int i = 0; i< length; i++)
            objects[i] = getTestObject(offset+i);
        return objects;
    }

}
