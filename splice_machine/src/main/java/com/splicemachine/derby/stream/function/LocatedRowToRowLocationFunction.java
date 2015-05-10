package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;

public class LocatedRowToRowLocationFunction extends SpliceFunction<SpliceOperation,LocatedRow, RowLocation> {
    private static final long serialVersionUID = 3988079974858059941L;

    public LocatedRowToRowLocationFunction() {
    }

    public LocatedRowToRowLocationFunction(OperationContext<SpliceOperation> operationContext, int[] keyColumns) {
        super(operationContext);
    }

    @Override
     public RowLocation call(LocatedRow row) throws Exception {
        return row.getRowLocation();
    }
}