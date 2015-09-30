package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.WindowFunction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

/**
 * Factory for spliceengine LastValueFunction.
 *
 * @author Jeff Cunningham
 *         Date: 9/30/15
 */
public class LastValueFunction extends WindowFunctionBase implements WindowFunction {

    @Override
    public WindowFunction setup(ClassFactory classFactory,
                                String functionName,
                                DataTypeDescriptor returnDataType) {
        super.setup(classFactory, functionName, returnDataType);
        return this;
    }

    @Override
    public WindowFunction newWindowFunction() {
        return super.newWindowFunction("com.splicemachine.derby.impl.sql.execute.operations.window.function.LastValueFunction");
    }
}
