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

package com.splicemachine.derby.stream.iterator.merge;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.log4j.Logger;
import org.spark_project.guava.collect.PeekingIterator;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public abstract class AbstractMergeJoinIterator implements Iterator<LocatedRow>, Iterable<LocatedRow> {
    private static final Logger LOG = Logger.getLogger(MergeOuterJoinIterator.class);
    final Iterator<LocatedRow> leftRS;
    final PeekingIterator<LocatedRow> rightRS;
    final int[] joinKeys;
    protected final OperationContext<?> operationContext;
    protected List<ExecRow> currentRights = new ArrayList<ExecRow>();
    protected LocatedRow left;
    protected Iterator<ExecRow> currentRightIterator;
    protected LocatedRow currentLocatedRow;
    protected JoinOperation mergeJoinOperation;
    private boolean closed = false;
    private List<Closeable> closeables = new ArrayList<>();
    private transient boolean populated =false;
    private transient boolean hasNext = false;

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
    public AbstractMergeJoinIterator(Iterator<LocatedRow> leftRS,
                                     PeekingIterator<LocatedRow> rightRS,
                                     int[] leftKeys, int[] rightKeys,
                                     JoinOperation mergeJoinOperation, OperationContext<?> operationContext) {
        this.mergeJoinOperation = mergeJoinOperation;
        this.leftRS = leftRS;
        this.rightRS = rightRS;
        assert(leftKeys.length == rightKeys.length);
        joinKeys = new int[leftKeys.length * 2];
        for (int i = 0, s = leftKeys.length; i < s; i++){
            // add keys indices for left & right like [L1, R1, L2, R2, ...],
            // incremented to work with 1-based ExecRow.getColumn()
            joinKeys[i * 2] = leftKeys[i] + 1;
            joinKeys[i * 2 + 1] = rightKeys[i] + 1;
        }
        this.operationContext = operationContext;
    }

    private int compare(ExecRow left, ExecRow right) throws StandardException {
        for (int i = 0, s = joinKeys.length; i < s; i = i + 2) {
            int result = left.getColumn(joinKeys[i])
                    .compare(right.getColumn(joinKeys[i + 1]));
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    protected Iterator<ExecRow> rightsForLeft(ExecRow left) throws IOException, StandardException {
        if (currentRights.size() > 0 // Check to see if we've already collected the right rows
                && compare(left, currentRights.get(0)) == 0){ // that match this left
            return currentRights.iterator();
        }
        // If not, look for the ones that do
        currentRights = new ArrayList<ExecRow>();
        while (rightRS.hasNext()){
            int comparison = compare(left, rightRS.peek().getRow());
            if (comparison == 0) { // if matches left, add to buffer
                currentRights.add(rightRS.next().getRow().getClone());
            } else if (comparison < 0) { // if is greater than left, stop
                break;
            } else {
                // if is less than left, read next right
                rightRS.next();
            }
        }
        return currentRights.iterator();
    }

    @Override
    public Iterator<LocatedRow> iterator() {
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
    public LocatedRow next() {
        if(!hasNext()) throw new NoSuchElementException();
        populated=false;
        return currentLocatedRow;
    }

    @Override
    public void remove() {
        throw new RuntimeException("Not Supported");
    }

    protected LocatedRow mergeRows(LocatedRow leftRow, ExecRow rightRow) {
        try {
            ExecRow execRow = JoinUtils.getMergedRow(leftRow.getRow(), rightRow == null ? mergeJoinOperation.getEmptyRow() : rightRow,
                    mergeJoinOperation.wasRightOuterJoin,
                    mergeJoinOperation.getExecutionFactory().getValueRow(
                            mergeJoinOperation.getRightNumCols() + mergeJoinOperation.getLeftNumCols()));
            LocatedRow lr = new LocatedRow(leftRow.getRowLocation(), execRow);
            mergeJoinOperation.setCurrentLocatedRow(lr);
            return lr;
        } catch (Exception e ) {
            throw new RuntimeException(e);
        }
    }

    public void registerCloseable(Closeable closeable) {
        closeables.add(closeable);
    }

}