/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.WindowOperation;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowContext;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.window.BaseFrameBuffer;
import com.splicemachine.derby.stream.window.WindowFrameBuffer;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.TaskContext;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static java.util.Collections.sort;

/**
 * Created by jleach on 4/24/15.
 */

public class MergeWindowFunction<Op extends WindowOperation> extends SpliceFlatMapFunction<Op, Tuple2<ExecRow, Iterable<ExecRow>>,ExecRow> implements Serializable {
    public MergeWindowFunction() {
    }

    public MergeWindowFunction(OperationContext<Op> operationContext, WindowAggregator[] aggregates) {
        super(operationContext);
    }

    @Override
    public Iterator<ExecRow> call(Tuple2<ExecRow, Iterable<ExecRow>> tuple) throws Exception {
        Iterable<ExecRow> locatedRows = tuple._2();
        List<ExecRow> partitionRows =new ArrayList<>();
        for(ExecRow lr:locatedRows){
            partitionRows.add(lr);
        }
        WindowContext windowContext = operationContext.getOperation().getWindowContext();

        sort(partitionRows, new LocatedRowComparator(windowContext.getKeyColumns(),
                                                     windowContext.getKeyOrders(),
                                                     windowContext.getNullOrderings()));

        // window logic
        final WindowFrameBuffer frameBuffer = BaseFrameBuffer.createFrameBuffer(
                windowContext.getWindowFunctions(),
                new LocatedToExecRowIter(partitionRows.iterator()),
                windowContext.getFrameDefinition(),
                windowContext.getSortColumns(),
                operationContext.getOperation().getExecRowDefinition().getClone());

        return new ExecRowToLocatedRowIterable(new Iterable<ExecRow>() {
            @Override public Iterator<ExecRow> iterator() {
                return IteratorUtils.asInterruptibleIterator(frameBuffer);
            }
        });
    }

    private class LocatedRowComparator implements Comparator<ExecRow> {
        private final ColumnComparator rowComparator;

        public LocatedRowComparator(int[] keyColumns, boolean[] keyOrders, boolean[] nullOrders) {
            this.rowComparator = new ColumnComparator(keyColumns, keyOrders, nullOrders);
        }

        @Override
        public int compare(ExecRow r1, ExecRow r2) {
            return rowComparator.compare(r1, r2);
        }
    }
    private static class LocatedToExecRowIter implements Iterator<ExecRow> {
        private final Iterator<ExecRow> delegate;

        public LocatedToExecRowIter(Iterator<ExecRow> delegate){
            this.delegate=delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public ExecRow next() {
            return delegate.next();
        }

        @Override
        public void remove() {
            delegate.remove();
        }
    }

    public static class ExecRowToLocatedRowIterable implements Iterable<ExecRow>, Iterator<ExecRow> {
        private Iterator<ExecRow> execRows;

        public ExecRowToLocatedRowIterable(Iterable<ExecRow> execRows) {
            this.execRows = execRows.iterator();
        }

        @Override
        public Iterator<ExecRow> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            return execRows.hasNext();
        }

        @Override
        public ExecRow next() {
            return execRows.next();
        }

        @Override
        public void remove() {
            execRows.remove();
        }
    }

}
