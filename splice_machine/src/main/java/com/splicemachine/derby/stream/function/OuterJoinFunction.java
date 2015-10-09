package com.splicemachine.derby.stream.function;

import com.google.common.base.Optional;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import scala.Tuple2;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 4/22/15.
 */
public class OuterJoinFunction<Op extends SpliceOperation> extends SpliceJoinFunction<Op, Tuple2<LocatedRow,Optional<LocatedRow>>, LocatedRow> {
    private static final long serialVersionUID = 3988079974858059941L;

    public OuterJoinFunction() {
    }

    public OuterJoinFunction(OperationContext<Op> operationContext) {
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
    public LocatedRow call(Tuple2<LocatedRow, Optional<LocatedRow>> tuple) throws Exception {
        checkInit();
        LocatedRow lr;
        if (tuple._2.isPresent())
            lr = new LocatedRow(JoinUtils.getMergedRow(tuple._1.getRow(),
                    tuple._2.get().getRow(),op.wasRightOuterJoin,executionFactory.getValueRow(numberOfColumns)));
        else
            lr = new LocatedRow(JoinUtils.getMergedRow(tuple._1.getRow(),
                    op.getEmptyRow(),op.wasRightOuterJoin,executionFactory.getValueRow(numberOfColumns)));
        op.setCurrentLocatedRow(lr);
        if (!op.getRestriction().apply(lr.getRow())) {
            lr = new LocatedRow(JoinUtils.getMergedRow(tuple._1.getRow(),
                    op.getEmptyRow(), op.wasRightOuterJoin, executionFactory.getValueRow(numberOfColumns)));
            op.setCurrentLocatedRow(lr);
            operationContext.recordFilter();
        }
        return lr;
    }
}
