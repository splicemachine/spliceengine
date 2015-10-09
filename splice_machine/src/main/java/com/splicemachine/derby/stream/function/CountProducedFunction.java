package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.stream.iapi.OperationContext;

public class CountProducedFunction extends SpliceFunction {
    public CountProducedFunction() {
    }

    public CountProducedFunction(OperationContext<?> operationContext) {
        super(operationContext);
    }

    @Override
    public Object call(Object o) throws Exception {
        operationContext.recordProduced();
        return o;
    }
}