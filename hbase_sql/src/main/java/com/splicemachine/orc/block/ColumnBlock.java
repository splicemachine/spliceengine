package com.splicemachine.orc.block;

import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;

/**
 *
 *
 */
public interface ColumnBlock {
    public ColumnVector getColumnVector();
    public void setColumnVector(ColumnVector columnVector);
    public DataType getDataType();
    public void setDataType(DataType dataType);
    public Object getObject(int i);
    public Object getTestObject(int i);
    public Object[] getTestObjectArray(int offset, int length);

}
