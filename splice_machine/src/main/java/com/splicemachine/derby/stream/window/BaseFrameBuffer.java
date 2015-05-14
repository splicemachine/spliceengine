package com.splicemachine.derby.stream.window;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.window.FrameDefinition;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.window.function.SpliceGenericWindowFunction;
import com.splicemachine.derby.utils.PartitionAwarePushBackIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by jyuan on 9/15/14.
 */
abstract public class BaseFrameBuffer implements WindowFrameBuffer{
    protected final long frameStart;
    protected final long frameEnd;
    private final WindowAggregator[] aggregators;
    private final ExecRow templateRow;

    protected int start;
    protected int end;
    protected int current;
    private ExecRow currentRow;
    protected ArrayList<ExecRow> rows;
    protected PeekingIterator<ExecRow> source;
    protected byte[] partition;
    protected int[] sortColumns;
    private boolean consumed;
    private boolean initialized;

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
    }

    public ExecRow next() {
        ExecRow result = currentRow;
        currentRow = null;
        return result;
    }

    private ExecRow nextInternal() {
        ExecRow row;
        try {
            if (!initialized) {
                reset();
                initialized = true;
            }
            if (current >= rows.size()) {
                consumed = true;
                return null;
            }
            row = rows.get(current);
            for (WindowAggregator aggregator : aggregators) {
                // For current row  and window, evaluate the window function
                int aggregatorColumnId = aggregator.getFunctionColumnId();
                int resultColumnId = aggregator.getResultColumnId();
                SpliceGenericWindowFunction function = (SpliceGenericWindowFunction) templateRow.getColumn(aggregatorColumnId).getObject();
                row.setColumn(resultColumnId, function.getResult().cloneValue(false));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return row;
    }

    public boolean hasNext() {
        if (consumed) {
            return false;
        }
        if (currentRow != null) {
            return true;
        }
        currentRow = nextInternal();
        if (currentRow == null && source.hasNext()) {
            // next frame
            initialized = false;
            currentRow = nextInternal();
        }


        if (currentRow == null) {
            consumed = true;
        } else {
            try {
                move();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return currentRow != null;
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
}
