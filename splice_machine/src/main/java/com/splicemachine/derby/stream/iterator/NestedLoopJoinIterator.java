/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.iterator;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.IterableJoinFunction;
import com.splicemachine.derby.stream.utils.StreamLogUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.Iterator;

@NotThreadSafe
public class NestedLoopJoinIterator<Op extends SpliceOperation> implements Iterator<LocatedRow>, Iterable<LocatedRow> {
    private static Logger LOG = Logger.getLogger(NestedLoopJoinIterator.class);
    private boolean populated;
    protected LocatedRow populatedRow;
    protected IterableJoinFunction iterableJoinFunction;

    public NestedLoopJoinIterator(IterableJoinFunction iterableJoinFunction) throws StandardException, IOException {
        this.iterableJoinFunction = iterableJoinFunction;
    }

    @Override
    public boolean hasNext() {
        if(populated) {
            return true;
        }
            populated = false;
            if (iterableJoinFunction.hasNext()) {
                ExecRow mergedRow = JoinUtils.getMergedRow(iterableJoinFunction.getLeftRow(),
                        iterableJoinFunction.getRightRow(),iterableJoinFunction.wasRightOuterJoin()
                        ,iterableJoinFunction.getExecutionFactory().getValueRow(iterableJoinFunction.getNumberOfColumns()));
                populatedRow = new LocatedRow(iterableJoinFunction.getLeftRowLocation(),mergedRow);
                populated = true;
            }
            StreamLogUtils.logOperationRecordWithMessage(iterableJoinFunction.getLeftLocatedRow(), iterableJoinFunction.getOperationContext(), "exhausted");
            return populated;
    }

    @Override
    public LocatedRow next() {
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
    public Iterator<LocatedRow> iterator() {
        return this;
    }
}