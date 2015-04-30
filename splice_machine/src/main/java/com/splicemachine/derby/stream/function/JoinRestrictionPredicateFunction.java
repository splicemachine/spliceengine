package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;

import javax.annotation.Nullable;

/**
 * Created by jleach on 4/22/15.
 */
public class JoinRestrictionPredicateFunction<Op extends SpliceOperation> extends SplicePredicateFunction<Op,LocatedRow> {
    protected JoinOperation joinOp;
    protected boolean initialized = false;
    public JoinRestrictionPredicateFunction() {
        super();
    }

    public JoinRestrictionPredicateFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public boolean apply(@Nullable LocatedRow locatedRow) {
        if (!initialized) {
            joinOp = (JoinOperation) operationContext.getOperation();
            initialized = true;
        }
        try {
            return joinOp.getRestriction().apply(locatedRow.getRow());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
