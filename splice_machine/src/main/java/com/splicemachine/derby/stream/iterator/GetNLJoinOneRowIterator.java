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
