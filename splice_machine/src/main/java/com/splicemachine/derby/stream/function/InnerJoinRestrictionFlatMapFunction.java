package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;

import scala.Tuple2;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Collections;
import java.util.Iterator;

/**
 * Created by jleach on 4/22/15.
 */
@NotThreadSafe
public class InnerJoinRestrictionFlatMapFunction<Op extends SpliceOperation> extends SpliceJoinFlatMapFunction<Op,Tuple2<LocatedRow,Iterable<LocatedRow>>,LocatedRow> {
    protected LocatedRow leftRow;
    protected LocatedRow rightRow;
    protected ExecRow mergedRow;
    public InnerJoinRestrictionFlatMapFunction() {
        super();
    }

    public InnerJoinRestrictionFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public Iterable<LocatedRow> call(Tuple2<LocatedRow, Iterable<LocatedRow>> tuple) throws Exception {
        checkInit();
        leftRow = tuple._1();
        Iterator<LocatedRow> it = tuple._2.iterator();
        while (it.hasNext()) {
            rightRow = it.next();
            mergedRow = JoinUtils.getMergedRow(leftRow.getRow(),
                    rightRow.getRow(), op.wasRightOuterJoin,
                    executionFactory.getValueRow(numberOfColumns));
            op.setCurrentRow(mergedRow);
            if (op.getRestriction().apply(mergedRow)) { // Has Row, abandon
                LocatedRow lr = new LocatedRow(rightRow.getRowLocation(),mergedRow);
                op.setCurrentLocatedRow(lr);
                return Collections.singletonList(lr);
            }
            operationContext.recordFilter();
        }
        return Collections.EMPTY_LIST;
    }
}