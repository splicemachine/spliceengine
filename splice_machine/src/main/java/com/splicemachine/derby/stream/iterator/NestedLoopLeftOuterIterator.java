package com.splicemachine.derby.stream.iterator;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.NLJOuterJoinFunction;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.Iterator;
@NotThreadSafe
public class NestedLoopLeftOuterIterator<Op extends SpliceOperation> implements Iterator<LocatedRow>, Iterable<LocatedRow> {
    private static Logger LOG = Logger.getLogger(NestedLoopLeftOuterIterator.class);
    private boolean populated;
    protected LocatedRow populatedRow;
    protected NLJOuterJoinFunction<Op> outerJoinFunction;

    public NestedLoopLeftOuterIterator(NLJOuterJoinFunction outerJoinFunction) throws StandardException, IOException {
        this.outerJoinFunction = outerJoinFunction;
    }

    @Override
    public boolean hasNext() {
        if(populated) {
            return true;
        }
            populated = false;
            if (outerJoinFunction.rightSideNLJIterator.hasNext()) {
                ExecRow mergedRow = JoinUtils.getMergedRow(outerJoinFunction.leftRow.getRow(),
                        outerJoinFunction.rightSideNLJIterator.next().getRow(),outerJoinFunction.op.wasRightOuterJoin
                        ,outerJoinFunction.executionFactory.getValueRow(outerJoinFunction.numberOfColumns));
                populatedRow = new LocatedRow(outerJoinFunction.leftRow.getRowLocation(),mergedRow);
                populated = true;
            }
            return populated;
    }

    @Override
    public LocatedRow next() {
        SpliceLogUtils.trace(LOG, "next row=%s",populatedRow);
        populated=false;
        outerJoinFunction.op.setCurrentLocatedRow(populatedRow);
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