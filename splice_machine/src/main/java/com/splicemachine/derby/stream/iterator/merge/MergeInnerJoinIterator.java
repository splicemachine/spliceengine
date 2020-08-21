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

package com.splicemachine.derby.stream.iterator.merge;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.log4j.Logger;
import splice.com.google.common.collect.PeekingIterator;

public class MergeInnerJoinIterator extends AbstractMergeJoinIterator {
    private static final Logger LOG = Logger.getLogger(MergeInnerJoinIterator.class);
    /**
     * MergeJoinRows constructor. Note that keys for left & right sides
     * are the join keys on which each side is sorted (not all of the
     * join keys).
     *  @param leftRS        Iterator for left side rows
     * @param rightRS       Iterator for right side rows
     * @param leftKeys      Join key(s) on which left side is sorted
     * @param rightKeys     Join Key(s) on which right side is sorted
     * @param operationContext
     */
    public MergeInnerJoinIterator(PeekingIterator<ExecRow> leftRS,
                                  PeekingIterator<ExecRow> rightRS,
                                  int[] leftKeys, int[] rightKeys,
                                  JoinOperation mergeJoinOperation, OperationContext<JoinOperation> operationContext) {
        super(leftRS,rightRS,leftKeys,rightKeys,mergeJoinOperation, operationContext);
    }

    @Override
    public boolean internalHasNext() {
        try {
            if (left != null) {
                while (currentRightIterator.hasNext()) {
                    ExecRow right = currentRightIterator.next();
                    currentExecRow = mergeRows(left, right);
                    if (mergeJoinOperation.getRestriction().apply(currentExecRow)) {
                        return true;
                    }
                    operationContext.recordFilter();
                }
            }
            while (leftRS.hasNext()) {
                if (left == null)
                    left = leftRS.next().getClone();
                else
                    left.transfer(leftRS.next());
                if (!joinColumnHasNull(left, true)) {
                    currentRightIterator = rightsForLeft(left);
                    while (currentRightIterator.hasNext()) {
                        currentExecRow = mergeRows(left, currentRightIterator.next());
                        if (mergeJoinOperation.getRestriction().apply(currentExecRow)) {
                            if (isSemiJoin) {
                                left = null; // only one joined row needed, force iteration on left side next time
                            }
                            return true;
                        }
                        operationContext.recordFilter();
                    }
                }
            }
            return false;
        }  catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
