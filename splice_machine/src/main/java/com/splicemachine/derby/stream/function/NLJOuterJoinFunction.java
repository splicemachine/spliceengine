package com.splicemachine.derby.stream.function;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iterator.NestedLoopLeftOuterIterator;
import com.splicemachine.derby.stream.utils.StreamLogUtils;
import com.splicemachine.derby.stream.utils.StreamUtils;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by jleach on 4/24/15.
 */
public class NLJOuterJoinFunction<Op extends SpliceOperation> extends SpliceJoinFlatMapFunction<Op, LocatedRow, LocatedRow> {
    public Iterator<LocatedRow> rightSideNLJIterator;
    public LocatedRow leftRow;

    public NLJOuterJoinFunction() {}

    public NLJOuterJoinFunction(OperationContext<Op> operationContext) {
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
    public Iterable<LocatedRow> call(LocatedRow from) throws Exception {
        checkInit();
        DataSet dataSet = null;
        try {
            leftRow = from;
            op.getRightOperation().openCore(EngineDriver.driver().processorFactory().localProcessor(null,op));
            rightSideNLJIterator = op.getRightOperation().getLocatedRowIterator();
            if (!rightSideNLJIterator.hasNext()) {
                // No Rows Right Side...
                ExecRow mergedRow = JoinUtils.getMergedRow(leftRow.getRow(),
                        op.getEmptyRow(), op.wasRightOuterJoin
                        , executionFactory.getValueRow(numberOfColumns));
                LocatedRow lr = new LocatedRow(leftRow.getRowLocation(),mergedRow.getClone());
                StreamLogUtils.logOperationRecordWithMessage(lr,operationContext,"outer - right side no rows");
                op.setCurrentLocatedRow(lr);
                return Collections.singletonList(lr);
            }
            return new NestedLoopLeftOuterIterator(this);
        } finally {
            if (op.getRightOperation()!= null)
                op.getRightOperation().close();
        }
    }
}
