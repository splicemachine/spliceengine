package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.WindowOperation;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowContext;
import com.splicemachine.derby.stream.iapi.OperationContext;
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
    public MergeWindowFunction() {
    }

    public MergeWindowFunction(OperationContext<Op> operationContext, WindowAggregator[] aggregates) {
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
    public Iterable<LocatedRow> call(Tuple2<ExecRow, Iterable<LocatedRow>> tuple) throws Exception {
        Iterable<LocatedRow> locatedRows = tuple._2();
        List<LocatedRow> partitionRows =new ArrayList<>();
        for(LocatedRow lr:locatedRows){
            partitionRows.add(lr);
        }
        WindowContext windowContext = operationContext.getOperation().getWindowContext();
        sort(partitionRows, new LocatedRowComparator(windowContext.getKeyColumns(), windowContext.getKeyOrders()));

        // window logic
        final WindowFrameBuffer frameBuffer = BaseFrameBuffer.createFrameBuffer(
                windowContext.getWindowFunctions(),
                new LocatedToExecRowIter(partitionRows.iterator()),
                windowContext.getFrameDefinition(),
                windowContext.getSortColumns(),
                operationContext.getOperation().getExecRowDefinition().getClone());

        return new ExecRowToLocatedRowIterable(new Iterable<ExecRow>() {
            @Override public Iterator<ExecRow> iterator() { return frameBuffer; }
        });
    }

    private class LocatedRowComparator implements Comparator<LocatedRow> {
        private final ColumnComparator rowComparator;

        public LocatedRowComparator(int[] keyColumns, boolean[] keyOrders) {
            this.rowComparator = new ColumnComparator(keyColumns, keyOrders, true);
        }

        @Override
        public int compare(LocatedRow r1, LocatedRow r2) {
            return rowComparator.compare(r1.getRow(), r2.getRow());
        }
    }
    private static class LocatedToExecRowIter implements Iterator<ExecRow> {
        private final Iterator<LocatedRow> delegate;

        public LocatedToExecRowIter(Iterator<LocatedRow> delegate){
            this.delegate=delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public ExecRow next() {
            return delegate.next().getRow();
        }

        @Override
        public void remove() {
            delegate.remove();
        }
    }

    public static class ExecRowToLocatedRowIterable implements Iterable<LocatedRow>, Iterator<LocatedRow> {
        private Iterator<ExecRow> execRows;

        public ExecRowToLocatedRowIterable(Iterable<ExecRow> execRows) {
            this.execRows = execRows.iterator();
        }

        @Override
        public Iterator<LocatedRow> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            return execRows.hasNext();
        }

        @Override
        public LocatedRow next() {
            return new LocatedRow(execRows.next());
        }

        @Override
        public void remove() {
            execRows.remove();
        }
    }

}
