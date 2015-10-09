package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.stream.iapi.OperationContext;

public class CountReadFunction extends SpliceFunction {
    public CountReadFunction() {
    }

    public CountReadFunction(OperationContext<?> operationContext) {
        super(operationContext);
    }

    @Override
    public Object call(Object o) throws Exception {
        operationContext.recordRead();
        return o;
    }
}