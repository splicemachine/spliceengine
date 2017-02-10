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

package com.splicemachine.derby.stream.function.merge;

import org.spark_project.guava.base.Function;
import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iterator.merge.AbstractMergeJoinIterator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.spark_project.guava.collect.Iterators;
import org.spark_project.guava.collect.PeekingIterator;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by jleach on 6/9/15.
 */
public abstract class AbstractMergeJoinFlatMapFunction extends SpliceFlatMapFunction<JoinOperation,Iterator<LocatedRow>,LocatedRow> {
    boolean initialized;
    protected JoinOperation joinOperation;

    public AbstractMergeJoinFlatMapFunction() {
        super();
    }

    public AbstractMergeJoinFlatMapFunction(OperationContext<JoinOperation> operationContext) {
        super(operationContext);
    }

    @Override
    public Iterator<LocatedRow> call(Iterator<LocatedRow> locatedRows) throws Exception {
        PeekingIterator<LocatedRow> leftPeekingIterator = Iterators.peekingIterator(locatedRows);
        if (!initialized) {
            joinOperation = getOperation();
            initialized = true;
            if (!leftPeekingIterator.hasNext())
                return Collections.EMPTY_LIST.iterator();
            initRightScan(leftPeekingIterator);
        }
        final SpliceOperation rightSide = joinOperation.getRightOperation();
        DataSetProcessor dsp =EngineDriver.driver().processorFactory().bulkProcessor(getOperation().getActivation(), rightSide);
        final Iterator<LocatedRow> rightIterator = Iterators.transform(rightSide.getDataSet(dsp).toLocalIterator(), new Function<LocatedRow, LocatedRow>() {
            @Override
            public LocatedRow apply(@Nullable LocatedRow locatedRow) {
                operationContext.recordJoinedRight();
                return locatedRow;
            }
        });
        ((BaseActivation)joinOperation.getActivation()).setScanStartOverride(null); // reset to null to avoid any side effects
        ((BaseActivation)joinOperation.getActivation()).setScanKeys(null);
        ((BaseActivation)joinOperation.getActivation()).setScanStopOverride(null);
        AbstractMergeJoinIterator iterator = createMergeJoinIterator(leftPeekingIterator,
                Iterators.peekingIterator(rightIterator),
                joinOperation.getLeftHashKeys(), joinOperation.getRightHashKeys(),
                joinOperation, operationContext);
        iterator.registerCloseable(new Closeable() {
            @Override
            public void close() throws IOException {
                try {
                    rightSide.close();
                } catch (StandardException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        return iterator;
    }

    private int[] getColumnOrdering(SpliceOperation op) throws StandardException {
        SpliceOperation operation = op;
        while (operation != null && !(operation instanceof ScanOperation)) {
            operation = operation.getLeftOperation();
        }
        assert operation != null;

        return ((ScanOperation)operation).getColumnOrdering();
    }

    private boolean isKeyColumn(int[] columnOrdering, int col) {
        for (int keyCol:columnOrdering) {
            if (col == keyCol)
                return true;
        }

        return false;
    }

    protected void initRightScan(PeekingIterator<LocatedRow> leftPeekingIterator) throws StandardException{
        ExecRow firstHashRow = joinOperation.getKeyRow(leftPeekingIterator.peek().getRow());
        ExecRow startPosition = joinOperation.getRightResultSet().getStartPosition();
        int[] columnOrdering = getColumnOrdering(joinOperation.getRightResultSet());
        int nCols = startPosition != null ? startPosition.nColumns():0;
        ExecRow scanStartOverride = null;
        int[] scanKeys = null;
        // If start row of right table scan has as many columns as key colummns of the table, cannot further
        // narrow down scan space, so return right tabel scan start row.
        if (nCols == columnOrdering.length) {
            scanStartOverride = startPosition;
            scanKeys = columnOrdering;
        }
        else {
            int[] rightHashKeys = joinOperation.getRightHashKeys();
            // Find valid hash column values to narrow down right scan. The valid hash columns must:
            // 1) not be used as a start key for inner table scan
            // 2) be consecutive
            // 3) be a key column
            LinkedList<Pair<Integer, Integer>> hashColumnIndexList = new LinkedList<>();
            for (int i = 0; i < rightHashKeys.length; ++i) {
                if (rightHashKeys[i] > nCols - 1) {
                    if ((hashColumnIndexList.isEmpty() || hashColumnIndexList.getLast().getValue() == rightHashKeys[i] - 1) &&
                            isKeyColumn(columnOrdering, rightHashKeys[i])) {
                        hashColumnIndexList.add(new ImmutablePair<Integer, Integer>(i, rightHashKeys[i]));
                    } else {
                        break;
                    }
                }
            }

            scanStartOverride = new ValueRow(nCols + hashColumnIndexList.size());
            if (startPosition != null) {
                for (int i = 1; i <= startPosition.nColumns(); ++i) {
                    scanStartOverride.setColumn(i, startPosition.getColumn(i));
                }
            }
            for (int i = 0; i < hashColumnIndexList.size(); ++i) {
                Pair<Integer, Integer> hashColumnIndex = hashColumnIndexList.get(i);
                int index = hashColumnIndex.getKey();
                scanStartOverride.setColumn(nCols + i + 1, firstHashRow.getColumn(index + 1));
            }

            // Scan key should include columns
            // 1) preceding the first hash column, these columns are in the form of "col=constant"
            // 2) all hash columns that are key columns
            scanKeys = new int[hashColumnIndexList.size() + rightHashKeys[0]];
            for (int i = 0; i < rightHashKeys[0]; ++i) {
                scanKeys[i] = i;
            }
            for (int i = 0; i < hashColumnIndexList.size(); ++i) {
                Pair<Integer, Integer> hashColumnIndex = hashColumnIndexList.get(i);
                int colPos = hashColumnIndex.getValue();
                scanKeys[rightHashKeys[0] + i] = colPos;
            }
        }

        ((BaseActivation)joinOperation.getActivation()).setScanStartOverride(scanStartOverride);
        ((BaseActivation)joinOperation.getActivation()).setScanKeys(scanKeys);
        if (startPosition != null) {
            ((BaseActivation)joinOperation.getActivation()).setScanStopOverride(startPosition);
        }

    }


    protected abstract AbstractMergeJoinIterator createMergeJoinIterator(PeekingIterator<LocatedRow> leftPeekingIterator,
                                                                         PeekingIterator<LocatedRow> rightPeekingIterator,
                                                                         int[] leftHashKeys, int[] rightHashKeys, JoinOperation joinOperation, OperationContext<JoinOperation> operationContext);
}
