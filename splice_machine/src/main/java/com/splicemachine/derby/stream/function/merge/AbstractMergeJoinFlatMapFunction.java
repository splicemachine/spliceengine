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

package com.splicemachine.derby.stream.function.merge;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.MergeJoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.ScanInformation;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iterator.merge.AbstractMergeJoinIterator;
import com.splicemachine.utils.Pair;
import org.spark_project.guava.base.Function;
import org.spark_project.guava.base.Preconditions;
import org.spark_project.guava.collect.Iterators;
import org.spark_project.guava.collect.PeekingIterator;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jleach on 6/9/15.
 */
public abstract class AbstractMergeJoinFlatMapFunction extends SpliceFlatMapFunction<JoinOperation,Iterator<ExecRow>, ExecRow> {
    boolean initialized;
    protected JoinOperation joinOperation;
    protected SpliceOperation leftSide;
    protected SpliceOperation rightSide;
    private PeekingIterator<ExecRow> leftPeekingIterator;
    private Iterator<ExecRow> mergeJoinIterator;

    public AbstractMergeJoinFlatMapFunction() {
        super();
    }

    public AbstractMergeJoinFlatMapFunction(OperationContext<JoinOperation> operationContext) {
        super(operationContext);
    }

    protected class BufferedMergeJoinIterator implements PeekingIterator<ExecRow> {
        protected static final int BUFFERSIZE=4500;
        private static final int INITIALCAPACITY=100;
        private ArrayList<ExecRow> bufferedRowList = new ArrayList<>(INITIALCAPACITY);
        private PeekingIterator<ExecRow> sourceIterator;
        private boolean hasPeeked;

        // A pointer to the next row to return when next() is called.
        private int bufferPosition;

        private void fillBuffer() throws StandardException {
            bufferPosition = 0;
            bufferedRowList.clear();
            for (int i = 0; i < BUFFERSIZE && sourceIterator.hasNext(); i++ ){
                bufferedRowList.add((ExecRow) sourceIterator.next().getClone());
            }
            if (!bufferedRowList.isEmpty() || !initialized)
                startNewRightSideScan();
        }

        public List<ExecRow> getBufferList() {
            return bufferedRowList;
        }

        protected BufferedMergeJoinIterator(Iterator<ExecRow> sourceIterator) throws StandardException {
            this.sourceIterator = Iterators.peekingIterator(sourceIterator);;
            fillBuffer();
        }

        public ExecRow peek() {
            hasPeeked = true;
            if (bufferHasNextRow())
                return bufferedRowList.get(bufferPosition);
            else
                return sourceIterator.peek();
        }

        public void remove() {
            Preconditions.checkState(!this.hasPeeked, "Can't remove after you've peeked at next");
            if (bufferHasNextRow())
                bufferedRowList.remove(bufferPosition);
            else
                sourceIterator.remove();
        }

        private boolean bufferHasNextRow() {
            return bufferPosition < bufferedRowList.size();
        }

        @Override
        public boolean hasNext() {
            if (bufferHasNextRow())
                return true;
            try {
                fillBuffer();
            }
            catch (StandardException e) {
                throw new RuntimeException(e);
            }
            return bufferHasNextRow();
        }

        @Override
        public ExecRow next() {
            hasPeeked = false;
            if (bufferHasNextRow())
                return bufferedRowList.get(bufferPosition++);

            try {
                fillBuffer();
            }
            catch (StandardException e) {
                throw new RuntimeException(e);
            }
            ExecRow retval = null;
            if (bufferHasNextRow())
                retval = bufferedRowList.get(bufferPosition++);

            return retval;
        }


        private void startNewRightSideScan() throws StandardException {
            if (!initialized) {
                joinOperation = getOperation();
                initialized = true;
            }
            Iterator<ExecRow> rightIterator;

            if (bufferedRowList.isEmpty())
                rightIterator = Collections.emptyIterator();
            else {
                initRightScan();
                rightSide = joinOperation.getRightOperation();
                rightSide.reset();
                DataSetProcessor dsp =EngineDriver.driver().processorFactory().bulkProcessor(getOperation().getActivation(), rightSide);
                rightIterator = Iterators.transform(rightSide.getDataSet(dsp).toLocalIterator(), new Function<ExecRow, ExecRow>() {
                    @Override
                    public ExecRow apply(@Nullable ExecRow locatedRow) {
                        operationContext.recordJoinedRight();
                        return locatedRow;
                    }
                });
            }
            ((BaseActivation)joinOperation.getActivation()).setScanStartOverride(null); // reset to null to avoid any side effects
            ((BaseActivation)joinOperation.getActivation()).setScanKeys(null);
            ((BaseActivation)joinOperation.getActivation()).setScanStopOverride(null);
            ((BaseActivation)joinOperation.getActivation()).setKeyRows(null);
            if (mergeJoinIterator == null) {
                leftSide = joinOperation.getLeftOperation();
                mergeJoinIterator = createMergeJoinIterator(this,
                Iterators.peekingIterator(rightIterator),
                joinOperation.getLeftHashKeys(), joinOperation.getRightHashKeys(),
                joinOperation, operationContext);
                ((AbstractMergeJoinIterator) mergeJoinIterator).registerCloseable(new Closeable() {
                    @Override
                    public void close() throws IOException {
                        try {
                            if (leftSide != null && !leftSide.isClosed())
                                leftSide.close();
                            if (rightSide != null && !rightSide.isClosed())
                                rightSide.close();
                            initialized = false;
                            leftSide = null;
                            rightSide = null;
                        } catch (StandardException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            }
            else
                ((AbstractMergeJoinIterator)mergeJoinIterator).reInitRightRS(Iterators.peekingIterator(rightIterator));
        }

        protected void initRightScan() throws StandardException{
            ExecRow firstHashRow = joinOperation.getKeyRow(peek());
            ExecRow startPosition = joinOperation.getRightResultSet().getStartPosition();
            int[] columnOrdering = getColumnOrdering(joinOperation.getRightResultSet());
            int nCols = startPosition != null ? startPosition.nColumns():0;
            ExecRow scanStartOverride;
            boolean firstTime = true;

            ((BaseActivation)joinOperation.getActivation()).setKeyRows(getKeyRows());

            /* To see if we can further restrict the scan of the right table by overiding the scan
               startPosition with the actual values of the left key if it is greater than the
               right startPosition.  If the left key is longer than the right startPosition, then
               expand the scan startPosition by the extra key columns from the left.
             */

            // this is 0-based array, the column positions in the array are also 0-based.
            int[] colToBaseTableMap = ((MergeJoinOperation)joinOperation).getRightHashKeyToBaseTableMap();

            // we cannot map the hash fields back to the base table column position, so cannot utilize the hash field value
            // to restrict the rigth table scan
            if (colToBaseTableMap == null)
                scanStartOverride = startPosition;
            else {

                ExecRow newStartKey = new ValueRow(columnOrdering.length);
                int[] rightHashKeys = joinOperation.getRightHashKeys();
                int[] rightHashKeySortOrders = ((MergeJoinOperation) joinOperation).getRightHashKeySortOrders();
                boolean rightKeyIsNull = false, leftKeyIsNull = false, replaceWithLeftRowVal = false;
                int len = 0;

                for (int i = 0; i < columnOrdering.length; i++) {
                    int keyColumnBaseTablePosition = columnOrdering[i];
                    int j = 0;
                    for (; j < rightHashKeys.length; j++) {
                        // both columnOrdering and colToBaseTableMap are 0-based
                        if (colToBaseTableMap[j] == keyColumnBaseTablePosition) {
                            if (firstTime) {
                                rightKeyIsNull =
                                    startPosition == null || i >= nCols ||
                                    isNullDataValue(startPosition.getColumn(i + 1));
                                leftKeyIsNull =
                                    isNullDataValue(firstHashRow.getColumn(j + 1));

                                // Replace the right key value to seek to with left row data value if:
                                // - The right key is null, in which case left is always >= right , or
                                // - If neither left key nor right key is null, compare
                                //   left and right key values, and do the replacement if:
                                //   - If the key is ascending and the current right key start
                                //     position is less than the left data value, or
                                //   - If the key is descending and the current right key start
                                //     position is greater than the left data value.
                                // As long as the left and right key column values are equal,
                                // keep doing comparisons until we find the partial key
                                // or full key which is truly greater than the other.
                                if (rightKeyIsNull) {
                                    replaceWithLeftRowVal = true;
                                    firstTime = false;
                                }
                                else if (!leftKeyIsNull) {
                                    boolean rightKeyIsAscending = isAscendingKey(rightHashKeySortOrders, j);
                                    replaceWithLeftRowVal =
                                        (rightKeyIsAscending ?
                                            (startPosition.getColumn(i + 1).
                                                compare(firstHashRow.getColumn(j + 1)) < 0) :
                                            (startPosition.getColumn(i + 1).
                                                compare(firstHashRow.getColumn(j + 1)) > 0));
                                    // As long as prior parts of the key are equal, keep comparing
                                    // latter parts of the key until we have inequality.
                                    firstTime = startPosition.getColumn(i + 1).
                                        compare(firstHashRow.getColumn(j + 1)) == 0;
                                }
                                else
                                    firstTime = false;
                            }
                            if (replaceWithLeftRowVal) {
                                // If for some reason there is no DVD created, break out right away
                                // without building the full start key, to prevent NPE.
                                if (firstHashRow.getColumn(j + 1) == null) {
                                    j = rightHashKeys.length;
                                    break;
                                }
                                newStartKey.setColumn(i + 1, firstHashRow.getColumn(j + 1));
                            } else {
                                if (i >= nCols || startPosition.getColumn(i + 1) == null) {
                                    j = rightHashKeys.length;
                                    break;
                                }
                                newStartKey.setColumn(i + 1, startPosition.getColumn(i + 1));
                            }
                            len++;
                            break;
                        }
                    }
                    //no match found for the given key column position
                    if (j >= rightHashKeys.length)
                        break;
                }

                scanStartOverride = new ValueRow(len);
                for (int i = 0; i < len; i++)
                    scanStartOverride.setColumn(i + 1, newStartKey.getColumn(i + 1));
            }

            ((BaseActivation)joinOperation.getActivation()).setScanStartOverride(scanStartOverride);

            // we can only set the stop key if start key is from equality predicate like "key=constant"
            if (startPosition != null) {
                ScanInformation<ExecRow>  scanInfo = joinOperation.getRightResultSet().getScanInformation();
                if (scanInfo != null && scanInfo.getSameStartStopPosition())
                    ((BaseActivation)joinOperation.getActivation()).setScanStopOverride(startPosition);
            }
        }

        // Takes a cache full of left rows and makes a list of the
        // corresponding right key rows.
        // If a right key row were to have a null value in any column,
        // it is not added to the list.
        protected ArrayList<Pair<ExecRow, ExecRow>>  getKeyRows() throws StandardException {
            int[] columnOrdering = getColumnOrdering(joinOperation.getRightResultSet());
            int[] rightHashKeys = joinOperation.getRightHashKeys();
            int[] colToBaseTableMap = ((MergeJoinOperation)joinOperation).getRightHashKeyToBaseTableMap();

            int[] rightToLeftKeyMap = new int[columnOrdering.length];

            int numKeyColumns = 0;
            for (int i = 0; i < columnOrdering.length; i++) {
                int keyColumnBaseTablePosition = columnOrdering[i];
                int j = 0;
                boolean foundKeyColumn = false;
                for (; j < rightHashKeys.length; j++) {
                    // both columnOrdering and colToBaseTableMap are 0-based
                    if (colToBaseTableMap[j] == keyColumnBaseTablePosition) {
                        rightToLeftKeyMap[i] = j;
                        foundKeyColumn = true;
                    }
                }
                if (!foundKeyColumn)
                    break;
                numKeyColumns++;
            }
            if (numKeyColumns == 0)
                return null;

            ArrayList<Pair<ExecRow, ExecRow>> rightKeyRows = new ArrayList<>();
            ExecRow newStartKey = new ValueRow(numKeyColumns);
            ExecRow previousStartKey = null;
            Pair<ExecRow, ExecRow> previousKeyPair = null, newKeyPair;

            List<ExecRow> bufferList = getBufferList();
            int lastItem = bufferList.size()-1;
            ExecRow row, previousRow = null;

            for (int rowIndex = 0; rowIndex <= lastItem; rowIndex++) {
                row = joinOperation.getKeyRow(bufferList.get(rowIndex));

                boolean addRow = true;
                for (int i = 0; i < numKeyColumns; i++) {
                    int j = rightToLeftKeyMap[i];
                    if (isNullDataValue(row.getColumn(j + 1))) {
                        addRow = false;
                        break;
                    }
                    else
                        newStartKey.setColumn(i + 1, row.getColumn(j + 1));
                }

                // Don't add the same key twice.
                if (addRow && row.equals(previousRow))
                    addRow = false;

                if (addRow) {
                    previousRow = row;
                    newKeyPair = new Pair<>();
                    newKeyPair.setFirst(newStartKey);
                    newKeyPair.setSecond(newStartKey);
                    rightKeyRows.add(newKeyPair);

                    newStartKey = new ValueRow(numKeyColumns);
                }
            }
            return rightKeyRows;
        }
    }

    @Override
    public Iterator<ExecRow> call(Iterator<ExecRow> locatedRows) throws Exception {
        if (leftPeekingIterator == null) {
            leftPeekingIterator = new BufferedMergeJoinIterator(locatedRows);
        }

        return mergeJoinIterator;
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

    private static boolean isNullDataValue(DataValueDescriptor dvd) {
        return dvd == null || dvd.isNull();
    }
    private static boolean isAscendingKey(int[] rightHashKeySortOrders, int keyPos) {
        boolean retval = rightHashKeySortOrders == null ||
                         rightHashKeySortOrders.length <= keyPos ||
                         rightHashKeySortOrders[keyPos] == 1;
        return retval;
    }

    protected abstract AbstractMergeJoinIterator createMergeJoinIterator(PeekingIterator<ExecRow> leftPeekingIterator,
                                                                         PeekingIterator<ExecRow> rightPeekingIterator,
                                                                         int[] leftHashKeys, int[] rightHashKeys, JoinOperation joinOperation, OperationContext<JoinOperation> operationContext);
}
