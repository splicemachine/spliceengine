package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.OperationContext;

/**
 * Created by jleach on 4/24/15.
 */
public class NLJFunction<Op extends SpliceOperation> extends SpliceFlatMapFunction<Op, LocatedRow, LocatedRow> {
    SpliceOperation rightOperation;

    public NLJFunction() {}

    public NLJFunction(OperationContext<Op> operationContext, SpliceOperation rightOperation) {
        super(operationContext);
        this.rightOperation = rightOperation;
    }


    @Override
    public Iterable<LocatedRow> call(LocatedRow from) throws Exception {
        return null;
    }
}
