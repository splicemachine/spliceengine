package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
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
public class NLJAntiJoinFunction<Op extends SpliceOperation> extends SpliceJoinFlatMapFunction<Op, LocatedRow, LocatedRow> {


    public NLJAntiJoinFunction() {}

    public NLJAntiJoinFunction(OperationContext<Op> operationContext) {
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
            dataSet = op.getRightOperation().getDataSet(StreamUtils.controlDataSetProcessor);
            Iterator<LocatedRow> rightSideNLJ = dataSet.toLocalIterator();
            if (rightSideNLJ.hasNext()) {
                StreamLogUtils.logOperationRecordWithMessage(from, operationContext, "anti-join filtered");
                return Collections.EMPTY_LIST;
            }
            ExecRow mergedRow = JoinUtils.getMergedRow(from.getRow(), op.getEmptyRow(),
                    op.wasRightOuterJoin, executionFactory.getValueRow(numberOfColumns));
            StreamLogUtils.logOperationRecordWithMessage(from, operationContext, "anti-join");
            op.setCurrentRow(mergedRow);
            op.setCurrentRowLocation(from.getRowLocation());
            return Collections.singletonList(new LocatedRow(from.getRowLocation(), mergedRow));
        } finally {
            if (dataSet!= null)
                dataSet.close();
        }

    }
}
