package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.RowOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;

/**
 * Created by jleach on 5/19/15.
 */
public class RowOperationFunction extends SpliceFunction<RowOperation,LocatedRow,LocatedRow> {

    public RowOperationFunction() {
        super();
    }

    public RowOperationFunction(OperationContext<RowOperation> operationContext) {
        super(operationContext);
    }

    @Override
    public LocatedRow call(LocatedRow o) throws Exception {
        RowOperation rowOp = operationContext.getOperation();
        return new LocatedRow(rowOp.getRow());
    }

}
