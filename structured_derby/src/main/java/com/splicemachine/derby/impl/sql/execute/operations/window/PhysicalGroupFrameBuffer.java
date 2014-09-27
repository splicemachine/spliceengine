package com.splicemachine.derby.impl.sql.execute.operations.window;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.utils.PartitionAwarePushBackIterator;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by jyuan on 9/15/14.
 */
public class PhysicalGroupFrameBuffer extends BaseFrameBuffer{

    public PhysicalGroupFrameBuffer (SpliceRuntimeContext runtimeContext,
                                     WindowAggregator[] aggregators,
                                     PartitionAwarePushBackIterator<ExecRow> source,
                                     FrameDefinition frameDefinition,
                                     int[] sortColumns,
                                     ExecRow templateRow) throws StandardException {
        super(runtimeContext, aggregators, source, frameDefinition, sortColumns, templateRow);
    }

    @Override
    protected void loadFrame() throws IOException, StandardException {
        long frameStart = frameDefinition.getFrameStart().getValue();
        long frameEnd = frameDefinition.getFrameEnd().getValue();
        start = end = 0;
        if (frameStart > 0) {
            start = (int) frameStart;
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

    @Override
    public void move() throws StandardException, IOException{
        FrameDefinition.FrameMode frameMode = frameDefinition.getFrameMode();
        long frameStart = frameDefinition.getFrameStart().getValue();
        long frameEnd = frameDefinition.getFrameEnd().getValue();

        // Increment the current index first
        current++;

        if (frameStart != Long.MIN_VALUE) {
            // Remove rows from the frame
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
