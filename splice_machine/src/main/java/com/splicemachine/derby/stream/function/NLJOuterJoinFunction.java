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

package com.splicemachine.derby.stream.function;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iterator.NestedLoopLeftOuterIterator;
import com.splicemachine.derby.stream.utils.StreamLogUtils;
import org.apache.commons.collections.iterators.SingletonIterator;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by jleach on 4/24/15.
 */
public class NLJOuterJoinFunction<Op extends SpliceOperation> extends SpliceJoinFlatMapFunction<Op, LocatedRow, LocatedRow> {
    public Iterator<LocatedRow> rightSideNLJIterator;
    public LocatedRow leftRow;

    public NLJOuterJoinFunction() {}

    public NLJOuterJoinFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
    }

    @Override
    public Iterator<LocatedRow> call(LocatedRow from) throws Exception {
        checkInit();
        DataSet dataSet = null;
        try {
            leftRow = from;
            op.getRightOperation().openCore(EngineDriver.driver().processorFactory().localProcessor(null,op));
            rightSideNLJIterator = op.getRightOperation().getLocatedRowIterator();
            if (!rightSideNLJIterator.hasNext()) {
                // No Rows Right Side...
                ExecRow mergedRow = JoinUtils.getMergedRow(leftRow.getRow(),
                        op.getEmptyRow(), op.wasRightOuterJoin
                        , executionFactory.getValueRow(numberOfColumns));
                LocatedRow lr = new LocatedRow(leftRow.getRowLocation(),mergedRow.getClone());
                StreamLogUtils.logOperationRecordWithMessage(lr,operationContext,"outer - right side no rows");
                op.setCurrentLocatedRow(lr);
                return new SingletonIterator(lr);
            }
            return new NestedLoopLeftOuterIterator(this);
        } finally {
            if (op.getRightOperation()!= null)
                op.getRightOperation().close();
        }
    }
}
