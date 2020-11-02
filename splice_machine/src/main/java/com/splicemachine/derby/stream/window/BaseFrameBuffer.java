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

package com.splicemachine.derby.stream.window;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.impl.sql.execute.operations.window.FrameDefinition;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.window.function.SpliceGenericWindowFunction;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import splice.com.google.common.collect.Iterators;
import splice.com.google.common.collect.PeekingIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jyuan on 9/15/14.
 */
@SuppressFBWarnings(value = "UUF_UNUSED_PUBLIC_OR_PROTECTED_FIELD", justification = "DB-9844")
abstract public class BaseFrameBuffer implements WindowFrameBuffer{
    protected final long frameStart;
    protected final long frameEnd;
    private final WindowAggregator[] aggregators;
    private final ExecRow templateRow;
    private ResultBuffer resultBuffer;

    protected int start;
    protected int end;
    protected int current;
    protected ArrayList<ExecRow> rows;
    protected PeekingIterator<ExecRow> source;
    protected int[] sortColumns;
    private boolean initialized;

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public static WindowFrameBuffer createFrameBuffer(
                                                      WindowAggregator[] aggregators,
                                                      Iterator<ExecRow> source,
                                                      FrameDefinition frameDefinition,
                                                      int[] sortColumns,
                                                      ExecRow templateRow) throws StandardException {

        FrameDefinition.FrameMode frameMode = frameDefinition.getFrameMode();
        PeekingIterator<ExecRow> peekingSource = Iterators.peekingIterator(source);
        if (frameMode == FrameDefinition.FrameMode.ROWS) {
            return new PhysicalGroupFrameBuffer(
                    aggregators, peekingSource, frameDefinition, sortColumns, templateRow);
        }
        else {
            return new LogicalGroupFrameBuffer(
                    aggregators, peekingSource, frameDefinition, sortColumns, templateRow);
        }
    }

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public BaseFrameBuffer (WindowAggregator[] aggregators,
                            PeekingIterator<ExecRow> source,
                            FrameDefinition frameDefinition,
                            int[] sortColumns,
                            ExecRow templateRow) throws StandardException {
        this.aggregators = aggregators;
        this.source = source;
        this.sortColumns = sortColumns;
        this.templateRow = templateRow;

        for (WindowAggregator aggregator: this.aggregators) {
            aggregator.initialize(this.templateRow);
        }
        // All aggregators in this frame buffer share the same over() clause
        // so should all have the same frame definition.
        // The frame definition will not change over the life of this frame buffer
        this.frameStart = frameDefinition.getFrameStart().getValue();
        this.frameEnd = frameDefinition.getFrameEnd().getValue();
        this.rows = new ArrayList<ExecRow>();
        this.resultBuffer = new ResultBuffer();
    }

    public ExecRow next() {
        return resultBuffer.next();
    }

    private ExecRow nextInternal() throws IOException, StandardException {
        ExecRow row;
        if (current >= rows.size()) {
            return null;
        }
        row = rows.get(current);
        for (WindowAggregator aggregator : aggregators) {
            // For current row  and window, evaluate the window function
            int aggregatorColumnId = aggregator.getFunctionColumnId();
            int resultColumnId = aggregator.getResultColumnId();
            SpliceGenericWindowFunction function = (SpliceGenericWindowFunction) templateRow.getColumn(aggregatorColumnId).getObject();
            /* for window frame like ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING, when we get to the last rows,
               there are no corresponding rows after them, so function.getResult() could return null
             */
            DataValueDescriptor resultVal = function.getResult();
            if (resultVal != null) {
                resultVal = resultVal.cloneValue(false);
                row.setColumn(resultColumnId, resultVal);
            }
        }
        this.resultBuffer.bufferResult(row);
        return row;
    }

    private void finishFrame() throws StandardException {
        for (WindowAggregator aggregator : aggregators) {
            SpliceGenericWindowFunction cachedAggregator = aggregator.getCachedAggregator();
            if (cachedAggregator != null) {
                List<DataValueDescriptor> results = cachedAggregator.finishFrame();
                if (results != null) {
                    int resultColumnId = aggregator.getResultColumnId();
                    resultBuffer.setColumnResults(resultColumnId, results);
                }
            }
        }
        resultBuffer.setFinished();
    }


    public boolean hasNext() {
        if (!initialized) {
            initialized = true;
            try {
                reset();
                while (nextInternal() != null) {
                    move();
                }
                finishFrame();
            } catch (Exception se) {
                throw new RuntimeException(se);
            }
        }
        return resultBuffer.hasNext();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    protected void add(ExecRow row) throws StandardException{
        for(WindowAggregator aggregator : aggregators) {
            aggregator.accumulate(row, templateRow);
        }
    }

    protected void removeInternal() throws StandardException {
        for(WindowAggregator aggregator : aggregators) {
            int aggregatorColumnId = aggregator.getFunctionColumnId();
            SpliceGenericWindowFunction windowFunction =
                    (SpliceGenericWindowFunction) templateRow.getColumn(aggregatorColumnId).getObject();
            windowFunction.remove();
        }
    }

    protected void reset() throws StandardException, IOException {
        rows = new ArrayList<ExecRow>();

        // Initialize window functions
        for (WindowAggregator aggregator : this.aggregators) {
            int aggregatorColumnId = aggregator.getFunctionColumnId();
            SpliceGenericWindowFunction windowFunction =
                    (SpliceGenericWindowFunction) templateRow.getColumn(aggregatorColumnId).getObject();
            windowFunction.reset();
            aggregator.initialize(templateRow);
        }

        // initializes frame buffer
        loadFrame();
    }

    abstract protected void loadFrame() throws IOException, StandardException;

    private static class ResultBuffer implements Iterator<ExecRow> {
        private final List<ExecRow> results = new ArrayList<>();
        private Iterator<ExecRow> resultItr;
        private boolean finished;

        void bufferResult(ExecRow resultRow) {
            results.add(resultRow);
        }

        boolean isFinished() {
            return finished;
        }

        void reset() {
            resultItr = null;
            results.clear();
            finished = false;
        }

        public void setFinished() {
            finished = true;
            resultItr = results.iterator();
        }

        public int size() {
            return results.size();
        }

        @Override
        public boolean hasNext() {
            return (resultItr != null && resultItr.hasNext());
        }

        @Override
        public ExecRow next() {
            if (resultItr == null || ! resultItr.hasNext()) {
                return null;
            }
            ExecRow resultRow = resultItr.next();
            if (! hasNext()) {
                reset();
            }
            return resultRow;
        }

        @Override
        public void remove() {
            // not implemented
        }

        public void setColumnResults(int resultColumnId, List<DataValueDescriptor> columnResults) {
            for (int i = 0; i < results.size(); i++) {
                ExecRow row = results.get(i);
                row.setColumn(resultColumnId, columnResults.get(i));
            }
        }
    }

}
