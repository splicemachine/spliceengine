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

import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.stream.iapi.OperationContext;
import scala.Tuple2;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by yxia on 12/1/19.
 */
public class CogroupFullOuterJoinRestrictionFlatMapFunction<Op extends SpliceOperation>
        extends SpliceJoinFlatMapFunction<Op, Tuple2<ExecRow,Tuple2<Iterable<ExecRow>,Iterable<ExecRow>>>,ExecRow> {
    private int[] hashKeys;

    public CogroupFullOuterJoinRestrictionFlatMapFunction() {
        super();
    }

    public CogroupFullOuterJoinRestrictionFlatMapFunction(OperationContext<Op> operationContext, int[] hashKeys) {
        super(operationContext);
        assert hashKeys!=null && hashKeys.length >0 : "Bad Hash Keys Passed into Null Filter Function";
        this.hashKeys = hashKeys;
    }

    @Override
    public Iterator<ExecRow> call(Tuple2<ExecRow,Tuple2<Iterable<ExecRow>, Iterable<ExecRow>>> tuple) throws Exception {
        ExecRow mergedRow;

        checkInit();
        List<ExecRow> returnRows = new LinkedList<>();
        BitSet matchingRights = new BitSet();
        for(ExecRow leftRow : tuple._2._1){

            Iterator<ExecRow> it = tuple._2._2.iterator();
            boolean leftHasMatch = false;
            // check if hashkey is null or not to prevent null=null to be qualified as true
            if (!hashKeyIsNull(leftRow)) {
                int rightIndex = 0;
                while (it.hasNext()) {
                    ExecRow rightRow = it.next();

                    mergedRow = JoinUtils.getMergedRow(leftRow,
                            rightRow, op.wasRightOuterJoin,
                            executionFactory.getValueRow(numberOfColumns));
                    mergedRow.setKey(leftRow.getKey());
                    op.setCurrentRow(mergedRow);
                    if (op.getRestriction().apply(mergedRow)) { // Has Row, abandon
                        if (!leftHasMatch)
                            leftHasMatch = true;
                        returnRows.add(mergedRow);
                        matchingRights.set(rightIndex);
                    }
                    operationContext.recordFilter();
                    rightIndex++;
                }
            }
            if (!leftHasMatch) {
                mergedRow = JoinUtils.getMergedRow(leftRow,
                            op.getRightEmptyRow(), op.wasRightOuterJoin,
                            executionFactory.getValueRow(numberOfColumns));
                mergedRow.setKey(leftRow.getKey());
                returnRows.add(mergedRow);
            }
        }
        // add the non-matching right rows
        Iterator<ExecRow> it = tuple._2._2.iterator();
        int rightIndex = 0;
        while (it.hasNext()) {
            ExecRow rightRow = it.next();
            if (!matchingRights.get(rightIndex)) {
                mergedRow = JoinUtils.getMergedRow(op.getLeftEmptyRow(),
                        rightRow, op.wasRightOuterJoin,
                        executionFactory.getValueRow(numberOfColumns));
                // TODO: DB-7810? what is the key?
                mergedRow.setKey(rightRow.getKey());
                returnRows.add(mergedRow);
            }
            rightIndex++;
        }

        return returnRows.iterator();
    }

    private boolean hashKeyIsNull(ExecRow row) {
        try {
            for (int i = 0; i < hashKeys.length; i++) {
                if (row.getColumn(hashKeys[i] + 1).isNull()) {
                    operationContext.recordFilter();
                    return true;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        ArrayUtil.writeIntArray(out,hashKeys);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        hashKeys = ArrayUtil.readIntArray(in);
    }
}
