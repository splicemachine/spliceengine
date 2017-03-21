package com.splicemachine.orc.block;

import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;

/**
 * Created by jleach on 3/17/17.
 */
public class LazyColumnBlock implements ColumnBlock {
    ColumnBlock delegate;

    public LazyColumnBlock(ColumnBlock delegate) {
        this.delegate = delegate;
    }

    @Override
    public ColumnVector getColumnVector() {
        return delegate.getColumnVector();
    }

    @Override
    public void setColumnVector(ColumnVector columnVector) {
        delegate.setColumnVector(columnVector);
    }

    @Override
    public DataType getDataType() {
        return delegate.getDataType();
    }

    @Override
    public void setDataType(DataType dataType) {
        delegate.setDataType(dataType);
    }

    @Override
    public Object getObject(int i) {
        return delegate.getObject(i);
    }

    @Override
    public Object getTestObject(int i) {
        return delegate.getTestObject(i);
    }

    @Override
    public Object[] getTestObjectArray(int offset, int length) {
        return delegate.getTestObjectArray(offset,length);
    }
}
