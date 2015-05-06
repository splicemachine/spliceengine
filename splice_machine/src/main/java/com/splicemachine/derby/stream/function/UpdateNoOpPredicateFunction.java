package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.UpdateOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import javax.annotation.Nullable;

/**
 *
 * Filters out rows where the rows have not changed.  This keeps a lot of writes from happening when the rows have
 * not changed.
 *
 */
public class UpdateNoOpPredicateFunction<Op extends SpliceOperation> extends SplicePredicateFunction<Op,LocatedRow> {
    protected UpdateOperation op;
    protected boolean initialized = false;
    public UpdateNoOpPredicateFunction() {
        super();
    }

    public UpdateNoOpPredicateFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public boolean apply(@Nullable LocatedRow locatedRow) {
        if (!initialized) {
            op = (UpdateOperation) operationContext.getOperation();
            initialized = true;
        }
        try {
            DataValueDescriptor[] sourRowValues = locatedRow.getRow().getRowArray();
            for (int i = op.getHeapList().anySetBit(), oldPos = 0; i >= 0; i = op.getHeapList().anySetBit(i), oldPos++) {
                DataValueDescriptor oldVal = sourRowValues[oldPos];
                DataValueDescriptor newVal = sourRowValues[op.getColumnPositionMap(op.getHeapList())[i]];
                if (!newVal.equals(oldVal)) {
                    return true; // Changed Columns...
                }
            }
            operationContext.recordFilter();
            return false; // No Columns Changed
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
