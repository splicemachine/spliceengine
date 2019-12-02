/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
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
public class LeftOuterJoinRestrictionFlatMapFunction<Op extends SpliceOperation> extends SpliceJoinFlatMapFunction<Op,Tuple2<ExecRow,Iterable<ExecRow>>,ExecRow> {
    protected ExecRow leftRow;
    protected ExecRow rightRow;
    protected ExecRow mergedRow;
    public LeftOuterJoinRestrictionFlatMapFunction() {
        super();
    }

    public LeftOuterJoinRestrictionFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public Iterator<ExecRow> call(Tuple2<ExecRow, Iterable<ExecRow>> tuple) throws Exception {
        checkInit();
        leftRow = tuple._1();
        List<ExecRow> returnRows = new ArrayList();
        Iterator<ExecRow> it = tuple._2.iterator();
        boolean hasMatch = false;
        while (it.hasNext()) {
            if (hasMatch && isSemiJoin)
                return returnRows.iterator();
            rightRow = it.next();
            mergedRow = JoinUtils.getMergedRow(leftRow,
                    rightRow, op.wasRightOuterJoin,
                    executionFactory.getValueRow(numberOfColumns));
            mergedRow.setKey(leftRow.getKey());
            op.setCurrentRow(mergedRow);
            if (op.getRestriction().apply(mergedRow)) { // Has Row, abandon
                if (!hasMatch)
                    hasMatch = true;
                else if (forSSQ) {
                    throw StandardException.newException(SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION);
                }
                returnRows.add(mergedRow);
            }
            operationContext.recordFilter();
        }
        if (returnRows.isEmpty()) {
            mergedRow = JoinUtils.getMergedRow(leftRow,
                    op.getRightEmptyRow(), op.wasRightOuterJoin,
                    executionFactory.getValueRow(numberOfColumns));
            mergedRow.setKey(leftRow.getKey());
            returnRows.add(mergedRow);
        }
        return returnRows.iterator();
    }
}
