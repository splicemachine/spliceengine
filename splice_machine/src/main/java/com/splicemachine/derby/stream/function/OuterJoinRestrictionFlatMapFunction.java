package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import scala.Tuple2;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jleach on 4/22/15.
 */
@NotThreadSafe
public class OuterJoinRestrictionFlatMapFunction<Op extends SpliceOperation> extends SpliceJoinFlatMapFunction<Op,Tuple2<LocatedRow,Iterable<LocatedRow>>,LocatedRow> {
    protected LocatedRow leftRow;
    protected LocatedRow rightRow;
    protected ExecRow mergedRow;
    public OuterJoinRestrictionFlatMapFunction() {
        super();
    }

    public OuterJoinRestrictionFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public Iterable<LocatedRow> call(Tuple2<LocatedRow, Iterable<LocatedRow>> tuple) throws Exception {
        checkInit();
        leftRow = tuple._1();
        List<LocatedRow> returnRows = new ArrayList();
        Iterator<LocatedRow> it = tuple._2.iterator();
        while (it.hasNext()) {
            rightRow = it.next();
            mergedRow = JoinUtils.getMergedRow(leftRow.getRow(),
                    rightRow.getRow(), op.wasRightOuterJoin,
                    executionFactory.getValueRow(numberOfColumns));
            op.setCurrentRow(mergedRow);
            if (op.getRestriction().apply(mergedRow)) { // Has Row, abandon
                LocatedRow lr = new LocatedRow(leftRow.getRowLocation(),mergedRow);
                returnRows.add(lr);
            }
        }
        if (returnRows.size() ==0) {
            mergedRow = JoinUtils.getMergedRow(leftRow.getRow(),
                    op.getEmptyRow(), op.wasRightOuterJoin,
                    executionFactory.getValueRow(numberOfColumns));
            LocatedRow lr = new LocatedRow(leftRow.getRowLocation(),mergedRow);
            returnRows.add(lr);
        }
        return returnRows;
    }
}