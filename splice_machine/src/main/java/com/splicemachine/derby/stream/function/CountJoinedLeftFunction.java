package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.stream.iapi.OperationContext;

public class CountJoinedLeftFunction extends SpliceFunction {
    public CountJoinedLeftFunction() {
    }

    public CountJoinedLeftFunction(OperationContext<?> operationContext) {
        super(operationContext);
    }

    @Override
    public Object call(Object o) throws Exception {
        operationContext.recordJoinedLeft();
        return o;
    }
}