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

/**
 * Factory for spliceengine RowNumberFunction.
 *
 * @author Jeff Cunningham
 *         Date: 8/4/14
 */
public class RowNumberFunction extends WindowFunctionBase implements WindowFunction {

    @Override
    public WindowFunction setup(ClassFactory classFactory, String aggregateName, DataTypeDescriptor returnDataType) {
        super.setup(classFactory, aggregateName, returnDataType);
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
        return super.newAggregator("com.splicemachine.derby.impl.sql.execute.operations.window.RowNumberFunction");
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
