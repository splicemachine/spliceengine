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

import com.splicemachine.EngineDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.utils.StreamLogUtils;
import com.splicemachine.utils.Pair;
import org.apache.commons.collections.iterators.SingletonIterator;

import java.util.Iterator;

/**
 * Created by jyuan on 10/10/16.
 */
public class GetNLJoinOneRowIterator extends GetNLJoinIterator {
    public GetNLJoinOneRowIterator() {}

    public GetNLJoinOneRowIterator(OperationContext operationContext, LocatedRow locatedRow) {
        super(operationContext, locatedRow);
    }

    @Override
    public Pair<OperationContext, Iterator<LocatedRow>> call() throws Exception {
        JoinOperation op = (JoinOperation) this.operationContext.getOperation();
        op.getLeftOperation().setCurrentLocatedRow(this.locatedRow);
        SpliceOperation rightOperation=op.getRightOperation();

        rightOperation.openCore(EngineDriver.driver().processorFactory().localProcessor(op.getActivation(), op));
        Iterator<LocatedRow> rightSideNLJIterator = rightOperation.getLocatedRowIterator();
        // Lets make sure we perform a call...
        boolean hasNext = rightSideNLJIterator.hasNext();
        if (hasNext) {
            // For left outer join, if there is no match, return an empty row from right side
            LocatedRow lr = rightSideNLJIterator.next();
            StreamLogUtils.logOperationRecordWithMessage(lr,operationContext,"outer - right side no rows");
            op.setCurrentLocatedRow(lr);
            rightSideNLJIterator = new SingletonIterator(lr);
        }

        return new Pair<>(operationContext, rightSideNLJIterator);
    }

}
