package com.splicemachine.derby.impl.sql.execute.operations.window;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.AggregateContext;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.utils.PartitionAwarePushBackIterator;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowFrame.*;
import java.io.IOException;
import java.util.ArrayList;

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
           SpliceGenericWindowFunction windowFunction =
                    (SpliceGenericWindowFunction)aggregator.getAggregatorInstance();
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

        if (frameMode == WindowFrame.FrameMode.RANGE) {
            loadRangeFrame(source);
        }
        else if (frameMode == WindowFrame.FrameMode.ROWS) {
            loadRowFrame(source);
        }

    }

    private void loadRowFrame(PartitionAwarePushBackIterator<ExecRow> source) throws IOException, StandardException {
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

    private void loadRangeFrame(PartitionAwarePushBackIterator<ExecRow> source) throws IOException, StandardException {

        long frameStart = windowFrame.getFrameStart().getValue();
        Frame frameStartType = windowFrame.getFrameStart().getFrame();
        long frameEnd = windowFrame.getFrameEnd().getValue();
        Frame frameEndType = windowFrame.getFrameEnd().getFrame();
        int[] sortColumn = windowContext.getSortColumns();

        //TODO: make sure there is only one column in the sort column
        // load first row into buffer
        ExecRow row = source.next(runtimeContext);
        DataValueDescriptor dvd = row.getColumn(sortColumn[0]+1);
        partition = source.getPartition();
        long value = dvd.getLong();
        // rows.add(row.getClone());
        // load enough rows into the buffer
        boolean done = false;
        start = 0;
        do {
            ExecRow clonedRow = null;
            if (row != null && Bytes.compareTo(partition, source.getPartition()) == 0) {
                clonedRow = row.getClone();
                dvd = row.getColumn(sortColumn[0]+1);
                long v = dvd.getLong();

                if (frameStart != Long.MIN_VALUE && value + frameStart > v) {
                    // We haven't seen the first row in the window yet, keep scanning
                    start++;
                }
                else if ((frameStart == Long.MIN_VALUE || frameStartType == Frame.CURRENT_ROW || value + frameStart >=v) &&
                    (frameEnd == Long.MAX_VALUE || frameEndType == Frame.CURRENT_ROW || value + frameEnd >= v)) {
                    add(clonedRow);
                }
                else if (frameEnd != Long.MAX_VALUE && value + frameEnd < v) {
                    // Stop scanner once a row is out of the range
                    done = true;
                }
            }
            else {
                // stop once a row from next partition is seen;
                done = true;
            }

            if (!done) {
                rows.add(clonedRow);
                if (frameEndType == Frame.CURRENT_ROW) {
                    break;
                }
                row = source.next(runtimeContext);
            }
            else {
                source.pushBack(row);
            }
        } while (!done);
        current = 0;
        end = rows.size()-1;
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
        FrameMode frameMode = windowFrame.getFrameMode();
        if (frameMode == FrameMode.ROWS) {
            moveRowFrame();
        }
        else if (frameMode == frameMode.RANGE) {
            moveRangeFrame();
        }
    }

    public void moveRangeFrame() throws StandardException, IOException {
        long frameStart = windowFrame.getFrameStart().getValue();
        long frameEnd = windowFrame.getFrameEnd().getValue();
        current++;
        if (rows.size() == current) {
            // scan next row into the buffer
            ExecRow row = source.next(runtimeContext);
            if (row != null && Bytes.compareTo(partition, source.getPartition()) == 0) {
                rows.add(row.getClone());
            }
            else {
                // We are either reach the end of a partition or
                // the end of a region
                source.pushBack(row);
                return;
            }

        }
        ExecRow nextRow = rows.get(current);
        int[] sortColumns = windowContext.getSortColumns();
        DataValueDescriptor dvd = nextRow.getColumn(sortColumns[0]+1);
        long value = dvd.getLong();

        // remove rows from window frame if they are out of range
        Frame f = windowFrame.getFrameStart().getFrame();
        if (f == Frame.CURRENT_ROW) {
            remove();
            start++;
        }
        else if (frameStart != Long.MIN_VALUE) {
            ExecRow startRow = rows.get(start);
            DataValueDescriptor d = startRow.getColumn(sortColumns[0]+1);
            long v = d.getLong();
            while (value + frameStart <= v && start < rows.size()) {
                remove();
                start++;
            }
        }

        // remove rows from buffer if they are no longer needed
        if (frameStart == Long.MIN_VALUE ) {
            // start frame is unbounded, remove the first row in the buffer
            rows.remove(0);
            start--;
            current--;
            end--;
        } else {

            int minIndex = (current < start) ? start : current;
            for (int i = 0; i < minIndex; ++i) {
                rows.remove(0);
                start--;
                current--;
                end--;
            }
        }

        // Add rows to window frame
        if (end != Long.MAX_VALUE) {
            boolean done = false;
            do {
                nextRow = getNext();
                if (nextRow != null) {
                    ExecRow clonedRow = nextRow.getClone();
                    DataValueDescriptor d = nextRow.getColumn(sortColumns[0] + 1);
                    long v = d.getLong();
                    if (value + frameEnd >= v || windowFrame.getFrameEnd().getFrame() == Frame.CURRENT_ROW) {
                        // If the row is in the range, add it to window
                        end++;
                        add(clonedRow);
                        if (windowFrame.getFrameEnd().getFrame() == Frame.CURRENT_ROW) {
                            done = true;
                        }
                    }
                    else {
                        // the row is out of range, push it back
                        done = true;
                        source.pushBack(nextRow);
                    }
                }
                else {
                    // consumed all rows
                    done = true;
                }
            } while (!done);
        }
    }

    private ExecRow getNext() throws StandardException, IOException{
        ExecRow row = null;
        if (end + 1 < rows.size()) {
            return rows.get(end+1);
        }
        else {
            // read a row from scanner
            row = source.next(runtimeContext);
            if (row == null || Bytes.compareTo(partition, source.getPartition())!=0) {
                source.pushBack(row);
                row = null;
            }
        }
        return row;
    }

    public void moveRowFrame() throws StandardException, IOException{

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