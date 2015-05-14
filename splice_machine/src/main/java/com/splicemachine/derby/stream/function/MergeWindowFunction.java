package com.splicemachine.derby.stream.function;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.IndexValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.WindowOperation;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowContext;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.spark.RDDUtils;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.derby.stream.window.BaseFrameBuffer;
import com.splicemachine.derby.stream.window.WindowFrameBuffer;
import scala.Tuple2;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.*;

import static java.util.Collections.sort;

/**
 * Created by jleach on 4/24/15.
 */

public class MergeWindowFunction<Op extends WindowOperation> extends SpliceFlatMapFunction<Op, Tuple2<ExecRow, Iterable<LocatedRow>>,LocatedRow> implements Serializable {
    protected WindowAggregator[] aggregates;
    public MergeWindowFunction() {
    }

    public MergeWindowFunction(OperationContext<Op> operationContext, WindowAggregator[] aggregates) {
        super(operationContext);
        this.aggregates = aggregates;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        aggregates = operationContext.getOperation().getWindowContext().getWindowFunctions();
    }

    @Override
    public Iterable<LocatedRow> call(Tuple2<ExecRow, Iterable<LocatedRow>> tuple) throws Exception {
        Iterable<LocatedRow> locatedRows = tuple._2();
        List<LocatedRow> partitionRows = Lists.newArrayList(locatedRows);
        WindowContext windowContext = operationContext.getOperation().getWindowContext();
        sort(partitionRows, new LocatedRowComparator(windowContext.getKeyColumns(), windowContext.getKeyOrders()));

        /// window logic
        final WindowFrameBuffer frameBuffer = BaseFrameBuffer.createFrameBuffer(
                windowContext.getWindowFunctions(),
                RDDUtils.toExecRowsIterator(partitionRows.iterator()),
                windowContext.getFrameDefinition(),
                windowContext.getSortColumns(),
                operationContext.getOperation().getExecRowDefinition().getClone());

        return RDDUtils.toSparkRowsIterable(new Iterable<ExecRow>() {
            @Override
            public Iterator<ExecRow> iterator() {
                return frameBuffer;
            }
        });
    }

    private class LocatedRowComparator implements Comparator<LocatedRow> {
        private final ColumnComparator rowComparator;

        public LocatedRowComparator(int[] keyColumns, boolean[] keyOrders) {
            this.rowComparator = new ColumnComparator(keyColumns, keyOrders, false);
        }

        @Override
        public int compare(LocatedRow r1, LocatedRow r2) {
            return rowComparator.compare(r1.getRow(), r2.getRow());
        }
    }
}
