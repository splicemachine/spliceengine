package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 4/22/15.
 */
public class AntiJoinFunction<Op extends SpliceOperation> extends SpliceJoinFunction<Op, LocatedRow, LocatedRow> {
    private static final long serialVersionUID = 3988079974858059941L;
    protected int numberOfColumns = 0;

    public AntiJoinFunction() {
    }

    public AntiJoinFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
    }

    @Override
    public LocatedRow call(LocatedRow inputRow) throws Exception {
        checkInit();
        LocatedRow lr = new LocatedRow(inputRow.getRowLocation(),JoinUtils.getMergedRow(inputRow.getRow(),
                op.getEmptyRow(), op.wasRightOuterJoin,executionFactory.getValueRow(numberOfColumns)));
        op.setCurrentLocatedRow(lr);
        return lr;
    }

}
