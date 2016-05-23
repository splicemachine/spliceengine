package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.WindowFunction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

/**
 * Factory for spliceengine FirstLastValueFunction.
 *
 * @author Jeff Cunningham
 *         Date: 9/30/15
 */
public class LeadLagFunction extends WindowFunctionBase implements WindowFunction {

    @Override
    public WindowFunction setup(ClassFactory classFactory,
                                String functionName,
                                DataTypeDescriptor returnDataType,
                                FormatableHashtable functionSpecificArgs) {
        super.setup(classFactory, functionName, returnDataType, functionSpecificArgs);
        return this;
    }

    @Override
    public WindowFunction newWindowFunction() {
        return super.newWindowFunction("com.splicemachine.derby.impl.sql.execute.operations.window.function.LeadLagFunction");
    }
}
