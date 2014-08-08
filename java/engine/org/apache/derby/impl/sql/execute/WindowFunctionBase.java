package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.sql.execute.WindowFunction;
import org.apache.derby.iapi.types.DataTypeDescriptor;

/**
 * Base class for Window Functions
 *
 * @author Jeff Cunningham
 *         Date: 8/6/14
 */
public abstract class WindowFunctionBase implements WindowFunction {

    protected ClassFactory classFactory;
    protected String aggregateName;
    protected DataTypeDescriptor inputType;
    protected DataTypeDescriptor returnDataType;

    @Override
    public WindowFunction setup(ClassFactory classFactory, String aggregateName, DataTypeDescriptor returnDataType) {
        this.classFactory = classFactory;
        this.aggregateName = aggregateName;
        this.returnDataType = returnDataType;
        return this;
    }

    public ExecAggregator newAggregator(String className) {
        ExecAggregator windowFunctionInstance = null;
        try{
            Class aggClass = classFactory.loadApplicationClass(className);
            Object agg = aggClass.newInstance();
            windowFunctionInstance = (WindowFunction)agg;
            windowFunctionInstance = windowFunctionInstance.setup(
                classFactory,
                aggregateName,
                returnDataType
            );
        }catch(Exception e){
            throw new RuntimeException(e);
        }
        return windowFunctionInstance;
    }
}
