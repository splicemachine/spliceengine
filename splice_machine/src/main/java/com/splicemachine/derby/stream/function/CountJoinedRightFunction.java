package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.stream.iapi.OperationContext;

public class CountJoinedRightFunction extends SpliceFunction {
    public CountJoinedRightFunction() {
    }

    public CountJoinedRightFunction(OperationContext<?> operationContext) {
        super(operationContext);
    }

    @Override
    public Object call(Object o) throws Exception {
        operationContext.recordJoinedRight();
        return o;
    }
}