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

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;

import scala.Tuple2;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Collections;
import java.util.Iterator;

/**
 * Created by jleach on 4/22/15.
 */
@NotThreadSafe
public class InnerJoinRestrictionFlatMapFunction<Op extends SpliceOperation> extends SpliceJoinFlatMapFunction<Op,Tuple2<LocatedRow,Iterable<LocatedRow>>,LocatedRow> {
    protected LocatedRow leftRow;
    protected LocatedRow rightRow;
    protected ExecRow mergedRow;
    public InnerJoinRestrictionFlatMapFunction() {
        super();
    }

    public InnerJoinRestrictionFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public Iterator<LocatedRow> call(Tuple2<LocatedRow, Iterable<LocatedRow>> tuple) throws Exception {
        checkInit();
        leftRow = tuple._1();
        Iterator<LocatedRow> it = tuple._2.iterator();
        while (it.hasNext()) {
            rightRow = it.next();
            mergedRow = JoinUtils.getMergedRow(leftRow.getRow(),
                    rightRow.getRow(), op.wasRightOuterJoin,
                    executionFactory.getValueRow(numberOfColumns));
            op.setCurrentRow(mergedRow);
            if (op.getRestriction().apply(mergedRow)) { // Has Row, abandon
                LocatedRow lr = new LocatedRow(rightRow.getRowLocation(),mergedRow);
                op.setCurrentLocatedRow(lr);
                return Collections.singletonList(lr).iterator();
            }
            operationContext.recordFilter();
        }
        return Collections.EMPTY_LIST.iterator();
    }
}