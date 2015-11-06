package com.splicemachine.derby.stream.window;

import com.google.common.collect.PeekingIterator;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.window.FrameDefinition;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowAggregator;
import java.io.IOException;

/**
 * @author  jyuan on 9/15/14.
 */
public class PhysicalGroupFrameBuffer extends BaseFrameBuffer {

    public PhysicalGroupFrameBuffer (
                                     WindowAggregator[] aggregators,
                                     PeekingIterator<ExecRow> source,
                                     FrameDefinition frameDefinition,
                                     int[] sortColumns,
                                     ExecRow templateRow) throws StandardException {
        super(aggregators, source, frameDefinition, sortColumns, templateRow);
    }

    @Override
    protected void loadFrame() throws IOException, StandardException {
        start = end = 0;
        if (frameStart > 0) {
            start = (int) frameStart;
        }

        for (int i = 0; i <= frameEnd; ++i) {
            if (!source.hasNext()) {
                break;
            }
            ExecRow row = source.next();
            ExecRow clonedRow = row.getClone();
            rows.add(clonedRow);

            // if the next row belongs to the same partition and falls
            // into the window range
            if (i >= frameStart)
                add(clonedRow);
        }
        current = 0;
        end = rows.size() -1;
    }

    @Override
    public void move() throws StandardException, IOException{
        // Increment the current index first
        current++;

        if (frameStart != Long.MIN_VALUE) {
            // Remove rows from the frame
            if (start < current + frameStart) {
                removeInternal();
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
            if (current + frameEnd > end) {
                // read a row from scanner
                if (source.hasNext()) {
                    ExecRow row = source.next();
                    ExecRow clonedRow = row.getClone();
                    rows.add(clonedRow);
                    // One more row is added into the frame buffer, include one more row into the window frame
                    end++;
                    add(rows.get(end));
                }
            }
        }
    }
}
