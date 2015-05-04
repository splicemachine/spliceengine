package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;

/**
 *
 *
 */
public class SetCurrentLocatedRowFunction<Op extends SpliceOperation> extends SpliceFunction<Op,LocatedRow,LocatedRow> {

    public SetCurrentLocatedRowFunction() {
        super();
    }

    public SetCurrentLocatedRowFunction(OperationContext operationContext) {
        super(operationContext);
    }

    @Override
    public LocatedRow call(LocatedRow locatedRow) throws Exception {
        getOperation().setCurrentLocatedRow(locatedRow);
        return locatedRow;
    }
}
