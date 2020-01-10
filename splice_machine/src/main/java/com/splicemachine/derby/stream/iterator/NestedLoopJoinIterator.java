/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.stream.iterator;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.stream.function.IteratorUtils;
import com.splicemachine.derby.stream.iapi.IterableJoinFunction;
import com.splicemachine.derby.stream.utils.StreamLogUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.Iterator;

@NotThreadSafe
public class NestedLoopJoinIterator<Op extends SpliceOperation> implements Iterator<ExecRow>, Iterable<ExecRow> {
    private static Logger LOG = Logger.getLogger(NestedLoopJoinIterator.class);
    private boolean populated;
    protected ExecRow populatedRow;
    protected IterableJoinFunction iterableJoinFunction;

    public NestedLoopJoinIterator(IterableJoinFunction iterableJoinFunction) throws StandardException, IOException {
        this.iterableJoinFunction = iterableJoinFunction;
        populatedRow = iterableJoinFunction.getExecutionFactory().getValueRow(iterableJoinFunction.getNumberOfColumns());
    }

    @Override
    public boolean hasNext() {
        if(populated) {
            return true;
        }
            populated = false;
            if (iterableJoinFunction.hasNext()) {
                populatedRow = JoinUtils.getMergedRow(iterableJoinFunction.getLeftRow(),
                        iterableJoinFunction.getRightRow(),iterableJoinFunction.wasRightOuterJoin()
                        ,populatedRow);
                populated = true;
            }
            StreamLogUtils.logOperationRecordWithMessage(iterableJoinFunction.getLeftLocatedRow(), iterableJoinFunction.getOperationContext(), "exhausted");
            return populated;
    }

    @Override
    public ExecRow next() {
        StreamLogUtils.logOperationRecord(populatedRow, iterableJoinFunction.getOperationContext());
        populated=false;
        iterableJoinFunction.setCurrentLocatedRow(populatedRow);
        return populatedRow;
    }

    @Override
    public void remove() {
        SpliceLogUtils.trace(LOG, "remove");
    }
    @Override
    public Iterator<ExecRow> iterator() {
        return IteratorUtils.asInterruptibleIterator(this);
    }
}
