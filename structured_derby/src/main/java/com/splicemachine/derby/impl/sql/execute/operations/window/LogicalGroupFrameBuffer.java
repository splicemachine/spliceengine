package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.io.IOException;
import java.util.List;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.utils.PartitionAwarePushBackIterator;

/**
 * Created by jyuan on 9/15/14.
 */
public class LogicalGroupFrameBuffer extends BaseFrameBuffer {

    public LogicalGroupFrameBuffer (SpliceRuntimeContext runtimeContext,
                                    List<WindowAggregator> aggregators,
                                    PartitionAwarePushBackIterator<ExecRow> source,
                                    FrameDefinition frameDefinition,
                                    int[] sortColumns,
                                    ExecRow templateRow) throws StandardException {
        super(runtimeContext, aggregators, source, frameDefinition, sortColumns, templateRow);
    }

    @Override
    protected void loadFrame() throws IOException, StandardException {
        // peak the first row
        ExecRow row = source.next(runtimeContext);
        source.pushBack(row);
        // TODO: throw an error when there are more than one sort columns
        DataValueDescriptor currentValue = null;
        if (frameEnd < Long.MAX_VALUE) {
            currentValue = row.getColumn(sortColumns[0] + 1).cloneValue(false);
        }
        //long endValue = frameEnd == Long.MAX_VALUE ? frameEnd : frameEnd + currentValue;
        endOfPartition = false;
        partition = source.getPartition();

        boolean endOfFrame = false;
        while (!endOfFrame) {
            row = source.next(runtimeContext);
            if (row == null) {
                // consumed all rows
                break;
            }

            if (Bytes.compareTo(partition, source.getPartition()) != 0) {
                // consumed this partition, push back the row that belongs to the next partition
                source.pushBack(row);
                endOfPartition = true;
                endOfFrame = true;
            }
            else {
                ExecRow clonedRow = row.getClone();

                if (frameEnd < Long.MAX_VALUE) {
                    // if frame end is not unbounded following, compare values
                    DataValueDescriptor v = clonedRow.getColumn(sortColumns[0]+1);
                    if (v.compare(currentValue)==0) {
                        // if the value falls into the window frame, aggregate it
                        add(clonedRow);
                        rows.add(clonedRow);
                    }
                    else {
                        endOfFrame = true;
                        source.pushBack(row);
                    }
                }
                else {
                    // Otherwise, always aggregate it
                    rows.add(clonedRow);
                    add(clonedRow);
                }
            }
        }
        current = 0;
        end = rows.size() -1;
    }

    @Override
    public void move() throws StandardException, IOException{
        // Increment the current index first
        current++;
        // if the next candidate row is not in the buffer yet, read it from scanner
        if (current >= rows.size()) {
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
            else {
                return;
            }
        }

        // Remove rows from the front of the window frame
        DataValueDescriptor newKey = null;
        if (frameEnd < Long.MAX_VALUE || frameStart > Long.MIN_VALUE) {
            newKey = rows.get(current).getColumn(sortColumns[0] + 1);
        }
        if (frameStart > Long.MIN_VALUE) {
            boolean inRange = false;
            while (!inRange) {
                ExecRow row = rows.get(start);
                DataValueDescriptor v = row.getColumn(sortColumns[0]+1);
                if (v.compare(newKey) < 0) {
                    remove();
                    start++;
                }
                else {
                    inRange = true;
                }
            }
        }

        // Remove rows from buffer if they are no longer needed
        int minIndex = current < start ? current : start;
        for (int i = 0; i < minIndex; ++i) {
            rows.remove(0);
            start--;
            current--;
            end--;

        }

        // Add rows to the end of window frame
        if (frameEnd < Long.MAX_VALUE) {
            boolean inRange = true;
            while(inRange && !endOfPartition) {
                ExecRow row = source.next(runtimeContext);
                if (row != null) {
                    ExecRow clonedRow = row.getClone();
                    if (Bytes.compareTo(source.getPartition(), partition) == 0) {
                        DataValueDescriptor v = row.getColumn(sortColumns[0]+1);
                        if (newKey.compare(v) == 0) {
                            rows.add(clonedRow);
                            add(clonedRow);
                            end++;
                        }
                        else {
                            inRange = false;
                            source.pushBack(row);
                        }

                    } else {
                        source.pushBack(row);
                        endOfPartition = true;
                    }
                }
                else {
                    endOfPartition = true;
                }
            }
        }
    }
}