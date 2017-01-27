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

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import scala.Tuple2;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 *
 */
@NotThreadSafe
public class OuterJoinRestrictionFlatMapFunction<Op extends SpliceOperation> extends SpliceJoinFlatMapFunction<Op,Tuple2<LocatedRow,Iterable<LocatedRow>>,LocatedRow> {
    protected LocatedRow leftRow;
    protected LocatedRow rightRow;
    protected ExecRow mergedRow;
    public OuterJoinRestrictionFlatMapFunction() {
        super();
    }

    public OuterJoinRestrictionFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public Iterator<LocatedRow> call(Tuple2<LocatedRow, Iterable<LocatedRow>> tuple) throws Exception {
        checkInit();
        leftRow = tuple._1();
        List<LocatedRow> returnRows = new ArrayList();
        Iterator<LocatedRow> it = tuple._2.iterator();
        while (it.hasNext()) {
            rightRow = it.next();
            mergedRow = JoinUtils.getMergedRow(leftRow.getRow(),
                    rightRow.getRow(), op.wasRightOuterJoin,
                    executionFactory.getValueRow(numberOfColumns));
            op.setCurrentRow(mergedRow);
            if (op.getRestriction().apply(mergedRow)) { // Has Row, abandon
                LocatedRow lr = new LocatedRow(leftRow.getRowLocation(),mergedRow);
                returnRows.add(lr);
            }
            operationContext.recordFilter();
        }
        if (returnRows.size() ==0) {
            mergedRow = JoinUtils.getMergedRow(leftRow.getRow(),
                    op.getEmptyRow(), op.wasRightOuterJoin,
                    executionFactory.getValueRow(numberOfColumns));
            LocatedRow lr = new LocatedRow(leftRow.getRowLocation(),mergedRow);
            returnRows.add(lr);
        }
        return returnRows.iterator();
    }
}