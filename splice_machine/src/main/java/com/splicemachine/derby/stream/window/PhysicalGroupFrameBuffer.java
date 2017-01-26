/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
import com.splicemachine.derby.impl.sql.execute.operations.window.FrameDefinition;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowAggregator;
import org.spark_project.guava.collect.PeekingIterator;
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
        if (frameEnd != Long.MAX_VALUE) {
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
