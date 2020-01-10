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
package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.stream.iapi.OperationContext;
import scala.Tuple2;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created by yxia on 12/3/19.
 */
@NotThreadSafe
public class LeftAntiJoinRestrictionFlatMapFunction <Op extends SpliceOperation> extends SpliceJoinFlatMapFunction<Op, Tuple2<ExecRow,Iterable<ExecRow>>,ExecRow> {
    protected ExecRow leftRow;
    protected ExecRow rightRow;
    protected ExecRow mergedRow;
    protected boolean rightAsLeft;
    public LeftAntiJoinRestrictionFlatMapFunction() {
        super();
    }

    public LeftAntiJoinRestrictionFlatMapFunction(OperationContext<Op> operationContext, boolean rightAsLeft) {
        super(operationContext);
        this.rightAsLeft = rightAsLeft;
    }

    @Override
    public Iterator<ExecRow> call(Tuple2<ExecRow, Iterable<ExecRow>> tuple) throws Exception {
        checkInit();
        leftRow = tuple._1();
        List<ExecRow> returnRows = new ArrayList();
        Iterator<ExecRow> it = tuple._2.iterator();
        while (it.hasNext()) {
            rightRow = it.next();
            mergedRow = JoinUtils.getMergedRow(leftRow,
                    rightRow, rightAsLeft,
                    executionFactory.getValueRow(numberOfColumns));
            mergedRow.setKey(leftRow.getKey());
            op.setCurrentRow(mergedRow);
            if (op.getRestriction().apply(mergedRow)) { // Has Row, abandon
                return Collections.emptyIterator();
            }
        }
        mergedRow = JoinUtils.getMergedRow(leftRow,
                    rightAsLeft?op.getLeftEmptyRow():op.getRightEmptyRow(), rightAsLeft,
                    executionFactory.getValueRow(numberOfColumns));
        mergedRow.setKey(leftRow.getKey());
            returnRows.add(mergedRow);
        return returnRows.iterator();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(rightAsLeft);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        rightAsLeft = in.readBoolean();
    }
}
