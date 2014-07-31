package com.splicemachine.derby.impl.sql.execute.operations.window;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.AggregateContext;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.utils.PartitionAwarePushBackIterator;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowFrameDefinition.FrameType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
/**
 * Created by jyuan on 7/27/14.
 */
public class WindowFunctionWorker {
    private PartitionAwarePushBackIterator<ExecRow> source;
    private byte[] partition;
    private SpliceRuntimeContext runtimeContext;
    private AggregateContext aggregateContext;
    private WindowFrameDefinition frame;
    private SpliceGenericAggregator[] aggregators;
    private ArrayList<ExecRow> rows;
    private ExecRow currentRow;
    private int currentPosition;
    private long startOffset;
    private long endOffset;
    private int windowStartPos;
    private int windowEndPos;
    private ExecRow resultRow;
    private boolean endOfPartition = false;

    public WindowFunctionWorker() {

    }

    public WindowFunctionWorker(SpliceRuntimeContext runtimeContext,
                                PartitionAwarePushBackIterator<ExecRow> source,
                                WindowFrameDefinition frame,
                                AggregateContext aggregateContext,
                                ExecRow templateRow) throws StandardException, IOException{
        this.runtimeContext = runtimeContext;
        this.source = source;
        this.partition = source.getPartition();
        this.frame = frame;
        this.aggregateContext = aggregateContext;
        this.rows = new ArrayList<ExecRow>();
        this.resultRow = templateRow.getClone();
        init();
    }

    private void init() throws StandardException, IOException{
        aggregators = aggregateContext.getAggregators();
        for (SpliceGenericAggregator aggregator:aggregators) {
            SpliceGenericWindowFunction windowFunction =
                    (SpliceGenericWindowFunction)aggregator.getAggregatorInstance();
            windowFunction.reset();
            aggregator.initialize(resultRow);
        }
        FrameType start = frame.getFrameStart();
        FrameType end = frame.getFrameEnd();
        startOffset = Long.MIN_VALUE;
        endOffset = Long.MAX_VALUE;

        switch (start.getFrame()) {
            case  UNBOUNDED_PRECEDING:
                break;
            case PRECEDING:
                startOffset = -1 * start.getValue();
                break;
            case CURRENT_ROW:
                startOffset = 0;
                break;
            case FOLLOWING:
                startOffset = start.getValue();
                break;
            case UNBOUNDED_FOLLOWING:
                // This is an error
                default:
                break;
        }

        switch(end.getFrame()) {
            case PRECEDING:
                endOffset = -1 * end.getValue();
                break;
            case CURRENT_ROW:
                endOffset = 0;
                break;
            case FOLLOWING:
                endOffset = end.getValue();
                break;
            case UNBOUNDED_FOLLOWING:
                break;
            case UNBOUNDED_PRECEDING:
            default:
                // This is an error
                break;
        }
        populateWindow(startOffset, endOffset);
    }

    private void populateWindow(long start, long end) throws StandardException, IOException{

        windowStartPos = windowEndPos = 0;

        // load rows following the current row if the window ends beyond the current row, until
        // all rows in the same partition or region are consumed
        if (start >= 0) {
            windowStartPos = (int)start;
        }

        if (end>=0) {
            windowEndPos = 0;
        }

        endOfPartition = false;
        for (int i = 0; i <= end; ++i) {
            ExecRow row = source.next(runtimeContext);
            if (row == null) {
                break;
            }
            if (Bytes.compareTo(partition, source.getPartition()) == 0) {
                ExecRow clonedRow = row.getClone();
                rows.add(clonedRow);

                // if the next row belongs to the same partition and falls
                // into the window range
                if (i >= start)
                    accumulate(clonedRow);
            }
            else {
                // we consumed this partition, push back the row that belongs to the next partition
                endOfPartition = true;
                source.pushBack(row);
                break;
            }
        }
        currentPosition = 0;
        windowEndPos = rows.size() -1;
    }

    public ExecRow next(SpliceRuntimeContext runtimeContext) throws StandardException, IOException {


        // empty region
        if (currentPosition >= rows.size()) {
            ExecRow nextRow = source.next(runtimeContext);
            if (nextRow == null) {
                return null; // all data are returned
            }
            else {
                // This is the end of a partition
                partition = source.getPartition();
                source.pushBack(nextRow);
                rows.clear();
                resetWindow();
                populateWindow(startOffset, endOffset);
            }
        }
        currentRow = rows.get(currentPosition);

        // Get window function results for the current row
        for (SpliceGenericAggregator aggregator:aggregators) {
            // For current row  and window, evaluate the window function
            int aggregatorColumnId = aggregator.getAggregatorColumnId();
            int resultColumnId = aggregator.getResultColumnId();
            SpliceGenericWindowFunction function = (SpliceGenericWindowFunction)resultRow.getColumn(aggregatorColumnId).getObject();
            currentRow.setColumn(resultColumnId, function.getResult().cloneValue(false));
        }

        // Move the window
        moveWindow();
        ExecRow retVal = currentRow;

        return retVal;
    }

    private void resetWindow() throws StandardException{
        for(SpliceGenericAggregator aggregator : aggregators) {
            int aggregatorColumnId = aggregator.getAggregatorColumnId();
            SpliceGenericWindowFunction windowFunction =
                    (SpliceGenericWindowFunction)resultRow.getColumn(aggregatorColumnId).getObject();
            windowFunction.reset();
        }
    }
    private void moveWindow() throws StandardException, IOException{
        currentPosition++;
        // Remove a row out of window if necessary
        if (startOffset != Long.MIN_VALUE) {
            // If window frame start is bonded, and a row falls out of the range of the window, remove it
            if (windowStartPos < currentPosition && currentPosition - windowStartPos + startOffset > 0 ||
                windowStartPos >= currentPosition && windowStartPos - currentPosition < startOffset) {

                remove();
                windowStartPos++;

            }
        }

        if (startOffset == Long.MIN_VALUE ||
            windowStartPos <= currentPosition && windowStartPos > 0 ||
            currentPosition <= windowStartPos) {
            // Remove a row of the list of rows if one of the condition is satisfied
            // 1) window start frame is unbound
            // 2) the first row in the list falls out of the window
            // 3) the current row has been output and no longer needed

            rows.remove(0);
            windowStartPos--;
            currentPosition--;
            windowEndPos--;
        }

        // Add one more row to the window if necessary
        if (endOffset != Long.MAX_VALUE) {
            if (windowEndPos < currentPosition &&
                currentPosition - windowEndPos + endOffset > 0 &&
                windowEndPos >= 0 && windowEndPos+1 < rows.size()) {
                // The row is already in the list of rows
                windowEndPos++;
                accumulate(rows.get(windowEndPos));
            }
            //else if(windowEndPos >= currentPosition && windowEndPos - currentPosition < endOffset && !endOfPartition) {
            else if(!endOfPartition) {
                ExecRow row = source.next(runtimeContext);
                if (row != null) {
                    ExecRow clonedRow = row.getClone();
                    if (Bytes.compareTo(source.getPartition(), partition) == 0) {
                        accumulate(clonedRow);
                        rows.add(clonedRow);
                        windowEndPos++;
                    } else {
                        source.pushBack(row);
                        endOfPartition = true;
                    }
                }
            }
        }
    }

    private void accumulate(ExecRow row) throws StandardException{
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
}
