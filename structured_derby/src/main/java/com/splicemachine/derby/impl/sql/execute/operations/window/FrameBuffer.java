package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.window.function.SpliceGenericWindowFunction;
import com.splicemachine.derby.utils.PartitionAwarePushBackIterator;

/**
 * Created by jyuan on 8/4/14.
 */
public class FrameBuffer {
    private final SpliceRuntimeContext runtimeContext;
    private final WindowAggregator[] aggregators;
    private final ExecRow templateRow;

    private PartitionAwarePushBackIterator<ExecRow> source;
    private int start;
    private int end;
    private int current;
    private ArrayList<ExecRow> rows;
    private FrameDefinition frameDefinition;
    private byte[] partition;
    private boolean endOfPartition;

    public FrameBuffer (SpliceRuntimeContext runtimeContext,
                        WindowAggregator[] aggregators,
                        PartitionAwarePushBackIterator<ExecRow> source,
                        FrameDefinition frameDefinition, ExecRow templateRow) throws StandardException {
        this.source = source;
        this.runtimeContext = runtimeContext;
        this.frameDefinition = frameDefinition;
        this.templateRow = templateRow;
        this.aggregators = aggregators;
        this.rows = new ArrayList<ExecRow>();

        for (WindowAggregator aggregator: this.aggregators) {
            SpliceGenericWindowFunction windowFunction = aggregator.findOrCreateNewWindowFunction();
            aggregator.initialize(this.templateRow);
        }
    }

    private void reset() throws StandardException, IOException {
        rows = new ArrayList<ExecRow>();

        // Initialize window functions
        for (WindowAggregator aggregator : this.aggregators) {
            int aggregatorColumnId = aggregator.getAggregatorColumnId();
            SpliceGenericWindowFunction windowFunction =
                    (SpliceGenericWindowFunction) templateRow.getColumn(aggregatorColumnId).getObject();
            windowFunction.reset();
            aggregator.initialize(templateRow);
        }

        // initializes frame buffer
        loadFrame();
    }

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

    public void move() throws StandardException, IOException{

        long frameStart = frameDefinition.getFrameStart().getValue();
        long frameEnd = frameDefinition.getFrameEnd().getValue();
        // Increment the current index first
        current++;

        if (frameStart != Long.MIN_VALUE) {
            // Remove one row from the frame
            if (start < current + frameStart) {
                remove();
                start++;
            }
        }


        // If the first row in the buffer is no longer needed, remove it
        if (frameStart == Long.MIN_VALUE || start > 0  || current <= start) {
            // In one of the three cases, the first row in the buffer is not needed
            // 1. frame start is unbounded
            // 2. the row was just moved out of th window frame
            // 3. window start frame is after the current row

            rows.remove(0);
            start--;
            current--;
            end--;
        }

        // Add a row to the window frame
        if (end != Long.MAX_VALUE) {
            if (current + frameEnd > end && !endOfPartition) {
                // read a row from scanner
                ExecRow row = source.next(runtimeContext);
                if (row != null) {
                    ExecRow clonedRow = row.getClone();
                    if (Bytes.compareTo(source.getPartition(), partition) == 0) {
                        rows.add(clonedRow);
                    } else {
                        source.pushBack(row);
                        endOfPartition = true;
                    }
                }
                else {
                    endOfPartition = true;
                }
                // If one more row is added into the frame buffer, include one more row into the window frame
                if (!endOfPartition) {
                    end++;
                    add(rows.get(end));
                }
            }
        }
    }

    //==========================================================================================
    // private
    //==========================================================================================

    private void loadFrame() throws IOException, StandardException {
        long frameStart = frameDefinition.getFrameStart().getValue();
        long frameEnd = frameDefinition.getFrameEnd().getValue();
        start = end = 0;
        if (frameStart > 0) {
            start = (int)frameStart;
        }
        endOfPartition = false;
        partition = source.getPartition();
        for (int i = 0; i <= frameEnd; ++i) {
            ExecRow row = source.next(runtimeContext);
            if (row == null) {
                break;
            }
            if (Bytes.compareTo(partition, source.getPartition()) == 0) {
                ExecRow clonedRow = row.getClone();
                rows.add(clonedRow);

                // if the next row belongs to the same partition and falls
                // into the window range
                if (i >= frameStart)
                    add(clonedRow);
            }
            else {
                // we consumed this partition, push back the row that belongs to the next partition
                source.pushBack(row);
                endOfPartition = true;
                break;
            }
        }
        current = 0;
        end = rows.size() -1;
    }

    private ExecRow next() throws IOException, StandardException{
        if (current >= rows.size()) {
            return null;
        }
        ExecRow row = rows.get(current);
        for (WindowAggregator aggregator : aggregators) {
            // For current row  and window, evaluate the window function
            int aggregatorColumnId = aggregator.getAggregatorColumnId();
            int resultColumnId = aggregator.getResultColumnId();
            SpliceGenericWindowFunction function = (SpliceGenericWindowFunction) templateRow.getColumn(aggregatorColumnId).getObject();
            row.setColumn(resultColumnId, function.getResult().cloneValue(false));
        }
        return row;
    }

    private void add(ExecRow row) throws StandardException{
        for(WindowAggregator aggregator : aggregators) {
            aggregator.accumulate(row, templateRow);
        }
    }

    private void remove() throws StandardException {
        for(WindowAggregator aggregator : aggregators) {
            int aggregatorColumnId = aggregator.getAggregatorColumnId();
            SpliceGenericWindowFunction windowFunction =
                (SpliceGenericWindowFunction) templateRow.getColumn(aggregatorColumnId).getObject();
            windowFunction.remove();
        }
    }
}