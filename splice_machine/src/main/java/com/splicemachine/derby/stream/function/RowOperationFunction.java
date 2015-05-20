package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.RowOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;

/**
 * Created by jleach on 5/19/15.
 */
public class RowOperationFunction extends SpliceFunction<SpliceOperation,LocatedRow,LocatedRow> {

    public RowOperationFunction() {
        super();
    }

    public RowOperationFunction(OperationContext<SpliceOperation> operationContext) {
        super(operationContext);
    }

    @Override
    public LocatedRow call(LocatedRow o) throws Exception {
        RowOperation rowOp = (RowOperation) operationContext.getOperation();
        System.out.println("Getting Located Row " + rowOp.getRow());
        return new LocatedRow(rowOp.getRow());
    }

}
