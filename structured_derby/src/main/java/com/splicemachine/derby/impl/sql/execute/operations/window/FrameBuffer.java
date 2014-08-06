package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.AggregateContext;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowFrame.Frame;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowFrame.FrameMode;
import com.splicemachine.derby.utils.PartitionAwarePushBackIterator;

/**
 * Created by jyuan on 8/4/14.
 */
public class FrameBuffer {
    private int start;
    private int end;
    private int current;
    private ArrayList<ExecRow> rows;
    private WindowFrame windowFrame;
    private SpliceGenericAggregator[] aggregators;
    private AggregateContext aggregateContext;
    private byte[] partition;
    private ExecRow resultRow;
    private SpliceRuntimeContext runtimeContext;
    private PartitionAwarePushBackIterator<ExecRow> source;
    private boolean endOfPartition;
    private WindowContext windowContext;

    public FrameBuffer (SpliceRuntimeContext runtimeContext,
                        AggregateContext aggregateContext,
                        PartitionAwarePushBackIterator<ExecRow> source,
                        WindowContext windowContext, ExecRow resultRow) throws StandardException {
        this.source = source;
        this.aggregateContext = aggregateContext;
        this.runtimeContext = runtimeContext;
        this.windowContext = windowContext;
        this.windowFrame = windowContext.getWindowFrame();
        this.resultRow = resultRow;
        aggregators = aggregateContext.getAggregators();
        for (SpliceGenericAggregator aggregator:aggregators) {
            SpliceGenericWindowFunction windowFunction = null;
            ExecAggregator function = aggregator.getAggregatorInstance();
            if (! (function instanceof SpliceGenericWindowFunction)) {
                windowFunction = (SpliceGenericWindowFunction) function.newAggregator();
            } else {
                windowFunction = (SpliceGenericWindowFunction)function;
            }
            windowFunction.reset();
            aggregator.initialize(resultRow);
        }
    }

    public void init(PartitionAwarePushBackIterator<ExecRow> source) throws StandardException, IOException {
        // Create a buffer for rows that are being worked on
        rows = new ArrayList<ExecRow>();

        // Initialize window functions
        for (SpliceGenericAggregator aggregator:aggregators) {
            int aggregatorColumnId = aggregator.getAggregatorColumnId();
            SpliceGenericWindowFunction windowFunction =
                    (SpliceGenericWindowFunction)resultRow.getColumn(aggregatorColumnId).getObject();
            windowFunction.reset();
            aggregator.initialize(resultRow);
        }

        // initializes frame buffer
        WindowFrame.FrameMode frameMode = windowFrame.getFrameMode();
        loadFrame(source);


    }

    private void loadFrame(PartitionAwarePushBackIterator<ExecRow> source) throws IOException, StandardException {
        long frameStart = windowFrame.getFrameStart().getValue();
        long frameEnd = windowFrame.getFrameEnd().getValue();
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

    public ExecRow next() throws IOException, StandardException{
        if (current >= rows.size()) {
            return null;
        }
        ExecRow row = rows.get(current);
        for (SpliceGenericAggregator aggregator:aggregators) {
            // For current row  and window, evaluate the window function
            int aggregatorColumnId = aggregator.getAggregatorColumnId();
            int resultColumnId = aggregator.getResultColumnId();
            SpliceGenericWindowFunction function = (SpliceGenericWindowFunction)resultRow.getColumn(aggregatorColumnId).getObject();
            row.setColumn(resultColumnId, function.getResult().cloneValue(false));
        }
        return row;
    }

    private void add(ExecRow row) throws StandardException{
        for(SpliceGenericAggregator aggregator : aggregators) {
            aggregator.accumulate(row, resultRow);
        }
    }

    private void remove() throws StandardException {
        for(SpliceGenericAggregator aggregator : aggregators) {
            int aggregatorColumnId = aggregator.getAggregatorColumnId();
            SpliceGenericWindowFunction windowFunction =
                    (SpliceGenericWindowFunction)resultRow.getColumn(aggregatorColumnId).getObject();
            windowFunction.remove();
        }
    }

    public void move() throws StandardException, IOException{

        long frameStart = windowFrame.getFrameStart().getValue();
        long frameEnd = windowFrame.getFrameEnd().getValue();
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
}