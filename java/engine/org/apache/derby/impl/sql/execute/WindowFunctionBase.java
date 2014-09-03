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
 * Base class for Window Functions that provides a factory interface to create and
 * initialize instances of the splice-side window functions.
 *
 * @author Jeff Cunningham
 *         Date: 8/6/14
 */
public abstract class WindowFunctionBase implements WindowFunction {

    protected ClassFactory classFactory;
    protected String windowFunctionName;
    protected DataTypeDescriptor returnDataType;

    @Override
    public ExecAggregator setup(ClassFactory classFactory,
                                String windowFunctionName,
                                DataTypeDescriptor returnDataType) {
        this.classFactory = classFactory;
        this.windowFunctionName = windowFunctionName;
        this.returnDataType = returnDataType;
        return this;
    }

    public WindowFunction newWindowFunction(String className) {
        WindowFunction windowFunctionInstance;
        try{
            Class windowFunctionClass = classFactory.loadApplicationClass(className);
            Object newInstance = windowFunctionClass.newInstance();
            windowFunctionInstance = (WindowFunction)newInstance;
            // the splice-side instance is invoked here
            windowFunctionInstance = (WindowFunction) windowFunctionInstance.setup(
                classFactory,
                windowFunctionName,
                returnDataType
            );
        }catch(Exception e){
            throw new RuntimeException(e);
        }
        return windowFunctionInstance;
    }

    // no-op interface implementations for derby side implementations


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
    public ExecAggregator newAggregator() {
        return null;
    }

    @Override
    public boolean didEliminateNulls() {
        return false;
    }

    @Override
    public void accumulate(DataValueDescriptor[] valueDescriptors) throws StandardException {

    }

    @Override
    public DataValueDescriptor getResult() throws StandardException {
        return null;
    }

    @Override
    public void reset() {}

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
