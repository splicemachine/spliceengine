package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.window.function.SpliceGenericWindowFunction;
import com.splicemachine.derby.utils.PartitionAwarePushBackIterator;

/**
 * Created by jyuan on 9/15/14.
 */
abstract public class BaseFrameBuffer implements WindowFrameBuffer{
    protected final SpliceRuntimeContext runtimeContext;
    private final List<WindowAggregator> aggregators;
    protected final long frameStart;
    protected final long frameEnd;
    private final ExecRow templateRow;

    protected int start;
    protected int end;
    protected int current;
    protected ArrayList<ExecRow> rows;
    protected PartitionAwarePushBackIterator<ExecRow> source;
    protected byte[] partition;
    protected boolean endOfPartition;
    protected int[] sortColumns;

    public static WindowFrameBuffer createFrameBuffer(SpliceRuntimeContext runtimeContext,
                                                      List<WindowAggregator> aggregators,
                                                      PartitionAwarePushBackIterator<ExecRow> source,
                                                      FrameDefinition frameDefinition,
                                                      int[] sortColumns,
                                                      ExecRow templateRow) throws StandardException {

        FrameDefinition.FrameMode frameMode = frameDefinition.getFrameMode();
        if (frameMode == FrameDefinition.FrameMode.ROWS) {
            return new PhysicalGroupFrameBuffer(
                    runtimeContext, aggregators, source, frameDefinition, sortColumns, templateRow);
        }
        else {
            return new LogicalGroupFrameBuffer(
                    runtimeContext, aggregators, source, frameDefinition, sortColumns, templateRow);
        }
    }

    public BaseFrameBuffer (SpliceRuntimeContext runtimeContext,
                            List<WindowAggregator> aggregators,
                            PartitionAwarePushBackIterator<ExecRow> source,
                            FrameDefinition frameDefinition,
                            int[] sortColumns,
                            ExecRow templateRow) throws StandardException {
        this.runtimeContext = runtimeContext;
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

    @Override
    public ExecRow next(SpliceRuntimeContext runtimeContext) throws StandardException, IOException {

        ExecRow row = this.next();

        if (row == null) {
            // This is the end of one partition, peek the next row
            row = source.next(runtimeContext);
            if (row == null) {
                // This is the end of the region
                return null;
            }
            else {
                // init a new window buffer for the next partition
                source.pushBack(row);
                this.reset();
                row = this.next();
            }
        }

        this.move();

        return row;
    }

    protected ExecRow next() throws IOException, StandardException{
        if (current >= rows.size()) {
            return null;
        }
        ExecRow row = rows.get(current);
        for (WindowAggregator aggregator : aggregators) {
            // For current row  and window, evaluate the window function
            int aggregatorColumnId = aggregator.getFunctionColumnId();
            int resultColumnId = aggregator.getResultColumnId();
            SpliceGenericWindowFunction function = (SpliceGenericWindowFunction) templateRow.getColumn(aggregatorColumnId).getObject();
            row.setColumn(resultColumnId, function.getResult().cloneValue(false));
        }
        return row;
    }

    protected void add(ExecRow row) throws StandardException{
        for(WindowAggregator aggregator : aggregators) {
            aggregator.accumulate(row, templateRow);
        }
    }

    protected void remove() throws StandardException {
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
