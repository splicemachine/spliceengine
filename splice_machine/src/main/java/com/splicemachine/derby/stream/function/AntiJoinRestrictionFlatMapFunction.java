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
public class AntiJoinRestrictionFlatMapFunction<Op extends SpliceOperation> extends SpliceJoinFlatMapFunction<Op,Tuple2<LocatedRow,Iterator<LocatedRow>>,LocatedRow> {
    protected LocatedRow leftRow;
    protected LocatedRow rightRow;
    protected ExecRow mergedRow;
    public AntiJoinRestrictionFlatMapFunction() {
        super();
    }

    public AntiJoinRestrictionFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public Iterable<LocatedRow> call(Tuple2<LocatedRow, Iterator<LocatedRow>> tuple) throws Exception {
        checkInit();
        leftRow = tuple._1();
        while (tuple._2.hasNext()) {
            rightRow = tuple._2.next();
            mergedRow = JoinUtils.getMergedRow(leftRow.getRow(),
                    rightRow.getRow(), op.wasRightOuterJoin,
                    executionFactory.getValueRow(numberOfColumns));
            op.setCurrentRow(mergedRow);
            if (op.getRestriction().apply(mergedRow)) // Has Row, abandon
                return Collections.EMPTY_LIST;
        }
        // No Rows Matched...
        LocatedRow returnRow = new LocatedRow(leftRow.getRowLocation(),JoinUtils.getMergedRow(leftRow.getRow(),
                op.getEmptyRow(), op.wasRightOuterJoin,
                executionFactory.getValueRow(numberOfColumns)));
        op.setCurrentLocatedRow(returnRow);
        return Collections.singletonList(returnRow);
    }
}
