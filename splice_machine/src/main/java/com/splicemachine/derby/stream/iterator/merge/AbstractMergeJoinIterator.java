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

package com.splicemachine.derby.stream.iterator.merge;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.MergeJoinOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.log4j.Logger;
import org.spark_project.guava.collect.PeekingIterator;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public abstract class AbstractMergeJoinIterator implements Iterator<ExecRow>, Iterable<ExecRow> {
    private static final Logger LOG = Logger.getLogger(MergeOuterJoinIterator.class);
    final PeekingIterator<ExecRow> leftRS;
    final PeekingIterator<ExecRow> rightRS;
    final int[] joinKeys;
    protected final OperationContext<?> operationContext;
    protected ExecRow left;
    protected List<ExecRow> currentRights = new ArrayList<ExecRow>();
    protected Iterator<ExecRow> currentRightIterator;
    protected ExecRow currentExecRow;
    protected JoinOperation mergeJoinOperation;
    private boolean closed = false;
    private List<Closeable> closeables = new ArrayList<>();
    private transient boolean populated =false;
    private transient boolean hasNext = false;
    protected ExecRow mergedRow;
    protected RightsForLeftsIterator rightsForLeftsIterator;
    protected boolean forSSQ = false;
    protected boolean isSemiJoin = false;
    protected int[] hashKeySortOrders;

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
    public AbstractMergeJoinIterator(PeekingIterator<ExecRow> leftRS,
                                     PeekingIterator<ExecRow> rightRS,
                                     int[] leftKeys, int[] rightKeys,
                                     JoinOperation mergeJoinOperation, OperationContext<?> operationContext) {
        this.mergeJoinOperation = mergeJoinOperation;
        this.mergedRow = mergeJoinOperation.getExecutionFactory().getValueRow(
                mergeJoinOperation.getRightNumCols() + mergeJoinOperation.getLeftNumCols());
        this.leftRS = leftRS;
        this.rightRS = rightRS;
        this.rightsForLeftsIterator = new RightsForLeftsIterator(rightRS);
        assert(leftKeys.length == rightKeys.length);
        joinKeys = new int[leftKeys.length * 2];
        for (int i = 0, s = leftKeys.length; i < s; i++){
            // add keys indices for left & right like [L1, R1, L2, R2, ...],
            // incremented to work with 1-based ExecRow.getColumn()
            joinKeys[i * 2] = leftKeys[i] + 1;
            joinKeys[i * 2 + 1] = rightKeys[i] + 1;
        }
        this.operationContext = operationContext;
        if (mergeJoinOperation.rightFromSSQ)
            forSSQ = true;
        if (mergeJoinOperation.isOneRowRightSide())
            isSemiJoin = true;
        hashKeySortOrders = ((MergeJoinOperation)mergeJoinOperation).getRightHashKeySortOrders();
    }

    private int compare(ExecRow left, ExecRow right) throws StandardException {
        for (int i = 0, s = joinKeys.length; i < s; i = i + 2) {
            int result = left.getColumn(joinKeys[i])
                    .compare(right.getColumn(joinKeys[i + 1]));
            //we need to handle cases where index columns are stored in descending order
            if (hashKeySortOrders != null && hashKeySortOrders[i/2] == 0) {
                result = -result;
            }

            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    private int compareLefts(ExecRow lastLeft, ExecRow nextLeft) throws StandardException {
        for (int i = 0, s = joinKeys.length; i < s; i = i + 2) {
            int result = lastLeft.getColumn(joinKeys[i])
                    .compare(nextLeft.getColumn(joinKeys[i]));
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    protected Iterator<ExecRow> rightsForLeft(ExecRow left) throws IOException, StandardException {
        if (!currentRights.isEmpty() // Check to see if we've already collected the right rows
                && compare(left, currentRights.get(0)) == 0){ // that match this left
            return currentRights.iterator();
        }

        if (leftRS.hasNext() && compareLefts(left,leftRS.peek()) == 0) { // Next Left
            // If not, look for the ones that do
            currentRights = new ArrayList<ExecRow>();
            while (rightRS.hasNext()) {
                int comparison = compare(left, rightRS.peek());
                if (comparison == 0) { // if matches left, add to buffer
                    currentRights.add(rightRS.next().getClone());
                } else if (comparison < 0) { // if is greater than left, stop
                    break;
                } else {
                    // if is less than left, read next right
                    rightRS.next();
                }
            }
            return currentRights.iterator();
        } else {
            rightsForLeftsIterator.setLeft(left);
            return rightsForLeftsIterator;
        }
    }

    @Override
    public Iterator<ExecRow> iterator() {
        return this;
    }

    @Override
    public final boolean hasNext() {
        if(populated) return hasNext;
        populated=true;
        boolean result =hasNext= internalHasNext();
        if (result) operationContext.recordProduced();
        if (!result && !closed) {
            close();
        }
        return result;
    }

    private void close() {
        closed = true;
        IOException e = null;
        for (Closeable c : closeables) {
            try {
                c.close();
            } catch (IOException ioe) {
                LOG.error("Error while closing " + c, e);
                e = ioe;
            }
        }
        if (e != null) {
            throw new RuntimeException(e);
        }
    }

    protected abstract boolean internalHasNext();

    @Override
    public ExecRow next() {
        if(!hasNext()) throw new NoSuchElementException();
        populated=false;
        return currentExecRow;
    }

    @Override
    public void remove() {
        throw new RuntimeException("Not Supported");
    }

    protected ExecRow mergeRows(ExecRow leftRow, ExecRow rightRow) {
        try {
            ExecRow execRow = JoinUtils.getMergedRow(leftRow, rightRow == null ? mergeJoinOperation.getEmptyRow() : rightRow,
                    mergeJoinOperation.wasRightOuterJoin,
                    mergedRow);
            mergeJoinOperation.setCurrentRow(execRow);
            return execRow;
        } catch (Exception e ) {
            throw new RuntimeException(e);
        }
    }

    public void registerCloseable(Closeable closeable) {
        closeables.add(closeable);
    }


    public class RightsForLeftsIterator implements Iterator<ExecRow>{
        private ExecRow leftRow;
        private PeekingIterator<ExecRow> rightRS;

        public RightsForLeftsIterator(PeekingIterator<ExecRow> rightRS) {
            this.rightRS = rightRS;
        }

        public void setLeft(ExecRow leftRow) throws StandardException {
            this.leftRow = leftRow;
        }

        @Override
        public boolean hasNext() {
            try {
                while (rightRS.hasNext()) {
                    // skip null rows
                    if (joinColumnHasNull(rightRS.peek(), false)) {
                        rightRS.next();
                        continue;
                    }

                    int comparison = compare(left, rightRS.peek());
                    if (comparison == 0) { // if matches left, add to buffer
                        return true;
                    } else if (comparison < 0) { // if is greater than left, stop
                        return false;
                    } else {
                        // if is less than left, read next right
                        rightRS.next();
                    }
                }
                return false;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public ExecRow next() {
            return rightRS.next();
        }
    }

    protected boolean joinColumnHasNull(ExecRow row, boolean isLeft) throws StandardException {
        int i=0;
        if (!isLeft)
            i++;
        for (; i < joinKeys.length; i = i + 2) {
            if (row.getColumn(joinKeys[i]).isNull())
                return true;
        }
        return false;
    }

}
