package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;

import javax.annotation.Nullable;

/**
 * Created by jleach on 4/22/15.
 */
public class JoinRestrictionPredicateFunction extends SplicePredicateFunction<JoinOperation,LocatedRow> {
    public JoinRestrictionPredicateFunction() {
        super();
    }

    public JoinRestrictionPredicateFunction(OperationContext<JoinOperation> operationContext) {
        super(operationContext);
    }

    @Override
    public boolean apply(@Nullable LocatedRow locatedRow) {
        JoinOperation joinOp = operationContext.getOperation();
        try {
            if (!joinOp.getRestriction().apply(locatedRow.getRow())) {
                operationContext.recordFilter();
                return false;
            }
            joinOp.setCurrentLocatedRow(locatedRow);
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
