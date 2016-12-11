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

package com.splicemachine.derby.stream.iterator.merge;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.log4j.Logger;
import org.spark_project.guava.collect.PeekingIterator;
import java.util.Iterator;

public class MergeOuterJoinIterator extends AbstractMergeJoinIterator {
    private static final Logger LOG = Logger.getLogger(MergeOuterJoinIterator.class);
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
    public MergeOuterJoinIterator(Iterator<LocatedRow> leftRS,
                                  PeekingIterator<LocatedRow> rightRS,
                                  int[] leftKeys, int[] rightKeys,
                                  JoinOperation mergeJoinOperation, OperationContext<?> operationContext) {
        super(leftRS,rightRS,leftKeys,rightKeys,mergeJoinOperation, operationContext);
    }

    @Override
    public boolean internalHasNext() {
        try {
            if (left != null) {
                while (currentRightIterator.hasNext()) {
                    ExecRow right = currentRightIterator.next();
                    currentLocatedRow = mergeRows(left, right);
                    if (mergeJoinOperation.getRestriction().apply(currentLocatedRow.getRow())) {
                        return true;
                    }
                    operationContext.recordFilter();
                }
            }
            while (leftRS.hasNext()) {
                left = leftRS.next();
                currentRightIterator = rightsForLeft(left.getRow());
                boolean returnedRows = false;
                while (currentRightIterator.hasNext()) {
                    currentLocatedRow = mergeRows(left, currentRightIterator.next());
                    if (mergeJoinOperation.getRestriction().apply(currentLocatedRow.getRow())) {
                        returnedRows = true;
                        return true;
                    }
                    operationContext.recordFilter();
                }
                if (!returnedRows) {
                    currentLocatedRow = mergeRows(left, null);
                    return true;
                }
            }
            return false;
        }  catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}