package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.sql.execute.WindowFunction;
import org.apache.derby.iapi.types.DataTypeDescriptor;

/**
 * Factory for spliceengine DenseRankFunction.
 *
 * @author Jeff Cunningham
 *         Date: 8/4/14
 */
public class DenseRankFunction extends WindowFunctionBase implements WindowFunction {

    @Override
    public WindowFunction setup(ClassFactory classFactory,
                                String functionName,
                                DataTypeDescriptor returnDataType) {
        super.setup(classFactory, functionName, returnDataType);
        return this;
    }

    @Override
    public WindowFunction newWindowFunction() {
        return super.newWindowFunction("com.splicemachine.derby.impl.sql.execute.operations.window.function.DenseRankFunction");
    }
}
