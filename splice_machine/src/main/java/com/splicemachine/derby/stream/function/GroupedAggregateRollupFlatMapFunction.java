package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.GroupedAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Arrays;

/**
 * Created by jleach on 4/22/15.
 */
@NotThreadSafe
public class GroupedAggregateRollupFlatMapFunction<Op extends SpliceOperation> extends SpliceFlatMapFunction<Op,LocatedRow,LocatedRow> {
    protected boolean initialized;
    protected GroupedAggregateOperation op;
    protected int[] groupColumns;

    public GroupedAggregateRollupFlatMapFunction() {
        super();
    }

    public GroupedAggregateRollupFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public Iterable<LocatedRow> call(LocatedRow from) throws Exception {
        if (!initialized) {
            initialized = true;
            op = (GroupedAggregateOperation) getOperation();
            groupColumns = op.groupedAggregateContext.getGroupingKeys();
        }
        LocatedRow[] rollupRows = new LocatedRow[groupColumns.length + 1];
        int rollUpPos = groupColumns.length;
        int pos = 0;
        ExecRow nextRow = from.getRow().getClone();
        do {
            rollupRows[pos] = new LocatedRow(nextRow);
            if (rollUpPos > 0) {
                nextRow = nextRow.getClone();
                DataValueDescriptor rollUpCol = nextRow.getColumn(groupColumns[rollUpPos - 1] + 1);
                rollUpCol.setToNull();
            }
            rollUpPos--;
            pos++;
        } while (rollUpPos >= 0);
        return Arrays.asList(rollupRows);
    }
}