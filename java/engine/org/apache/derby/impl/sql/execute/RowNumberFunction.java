package org.apache.derby.impl.sql.execute;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.sql.execute.WindowFunction;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLLongint;

/**
 * Implementation of ROW_NUMBER -  Assigns a sequential number to each row in the result.
 *
 * @author Jeff Cunningham
 *         Date: 8/4/14
 */
public class RowNumberFunction implements WindowFunction {

    @Override
    public DataValueDescriptor apply(DataValueDescriptor leftDvd,
                                     DataValueDescriptor rightDvd,
                                     DataValueDescriptor previousValue) throws StandardException {
        DataValueDescriptor result = null;
        if (previousValue == null || previousValue.isNull()) {
            result = new SQLLongint(1);
        } else {
            // TODO - ...
        }
        return result;
    }

    @Override
    public WindowFunction setup(ClassFactory classFactory, String aggregateName, DataTypeDescriptor returnDataType) {
        return this;
    }

    @Override
    public void accumulate(DataValueDescriptor addend, Object ga) throws StandardException {

    }

    @Override
    public void add(DataValueDescriptor addend) throws StandardException {

    }

    @Override
    public void merge(ExecAggregator inputAggregator) throws StandardException {

    }

    @Override
    public DataValueDescriptor getResult() throws StandardException {
        return null;
    }

    @Override
    public ExecAggregator newAggregator() {
        Class<?> clazz = null;
        try {
            clazz = Class.forName("com.splicemachine.derby.impl.sql.execute.operations.window.RowNumberFunction");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        Object instance = null;
        try {
            instance = clazz.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        return (ExecAggregator)instance;
    }

    @Override
    public boolean didEliminateNulls() {
        return false;
    }

    @Override
    public WindowFunction newWindowFunction() {
        return (WindowFunction) newAggregator();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }

    @Override
    public int getTypeFormatId() {
        return 0;
    }
}
