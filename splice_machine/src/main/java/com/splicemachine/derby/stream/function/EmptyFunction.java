package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.stream.iapi.OperationContext;

public class EmptyFunction extends SpliceFunction {
    public EmptyFunction() {
    }

    public EmptyFunction(OperationContext<?> operationContext) {
        super(operationContext);
    }

    @Override
    public Object call(Object o) throws Exception {
        return o;
    }
}