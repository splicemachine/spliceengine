package com.splicemachine.derby.stream.iterator;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.IterableJoinFunction;
import com.splicemachine.derby.stream.utils.StreamLogUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.Iterator;

@NotThreadSafe
public class NestedLoopInnerIterator<Op extends SpliceOperation> implements Iterator<LocatedRow>, Iterable<LocatedRow> {
    private static Logger LOG = Logger.getLogger(NestedLoopInnerIterator.class);
    private boolean populated;
    protected LocatedRow populatedRow;
    protected IterableJoinFunction iterableJoinFunction;

    public NestedLoopInnerIterator(IterableJoinFunction iterableJoinFunction) throws StandardException, IOException {
        this.iterableJoinFunction = iterableJoinFunction;
    }

    @Override
    public boolean hasNext() {
        if(populated) {
            return true;
        }
            populated = false;
            if (iterableJoinFunction.hasNext()) {
                ExecRow mergedRow = JoinUtils.getMergedRow(iterableJoinFunction.getLeftRow(),
                        iterableJoinFunction.getRightRow(),iterableJoinFunction.wasRightOuterJoin()
                        ,iterableJoinFunction.getExecutionFactory().getValueRow(iterableJoinFunction.getNumberOfColumns()));
                populatedRow = new LocatedRow(iterableJoinFunction.getLeftRowLocation(),mergedRow);
                populated = true;
            }
            StreamLogUtils.logOperationRecordWithMessage(iterableJoinFunction.getLeftLocatedRow(), iterableJoinFunction.getOperationContext(), "exhausted");
            return populated;
    }

    @Override
    public LocatedRow next() {
        StreamLogUtils.logOperationRecord(populatedRow, iterableJoinFunction.getOperationContext());
        populated=false;
        iterableJoinFunction.setCurrentLocatedRow(populatedRow);
        return populatedRow;
    }

    @Override
    public void remove() {
        SpliceLogUtils.trace(LOG, "remove");
    }
    @Override
    public Iterator<LocatedRow> iterator() {
        return this;
    }
}