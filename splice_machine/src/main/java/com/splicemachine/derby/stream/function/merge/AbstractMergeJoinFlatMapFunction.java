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

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
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
import org.spark_project.guava.base.Function;
import org.spark_project.guava.collect.Iterators;
import org.spark_project.guava.collect.PeekingIterator;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by jleach on 6/9/15.
 */
public abstract class AbstractMergeJoinFlatMapFunction extends SpliceFlatMapFunction<JoinOperation,Iterator<ExecRow>,ExecRow> {
    boolean initialized;
    protected JoinOperation joinOperation;

    public AbstractMergeJoinFlatMapFunction() {
        super();
    }

    public AbstractMergeJoinFlatMapFunction(OperationContext<JoinOperation> operationContext) {
        super(operationContext);
    }

    @Override
    public Iterator<ExecRow> call(Iterator<ExecRow> locatedRows) throws Exception {
        PeekingIterator<ExecRow> leftPeekingIterator = Iterators.peekingIterator(locatedRows);
        if (!initialized) {
            joinOperation = getOperation();
            initialized = true;
            if (!leftPeekingIterator.hasNext())
                return Collections.EMPTY_LIST.iterator();
            initRightScan(leftPeekingIterator);
        }
        final SpliceOperation rightSide = joinOperation.getRightOperation();
        rightSide.reset();
        DataSetProcessor dsp =EngineDriver.driver().processorFactory().bulkProcessor(getOperation().getActivation(), rightSide);
        final Iterator<ExecRow> rightIterator = Iterators.transform(rightSide.getDataSet(dsp).toLocalIterator(), new Function<ExecRow, ExecRow>() {
            @Override
            public ExecRow apply(@Nullable ExecRow locatedRow) {
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

    protected void initRightScan(PeekingIterator<ExecRow> leftPeekingIterator) throws StandardException{
        ExecRow firstHashRow = joinOperation.getKeyRow(leftPeekingIterator.peek());
        ExecRow startPosition = joinOperation.getRightResultSet().getStartPosition();
        int[] columnOrdering = getColumnOrdering(joinOperation.getRightResultSet());
        int nCols = startPosition != null ? startPosition.nColumns():0;
        ExecRow scanStartOverride;

        /* To see if we can further restrict the scan of the right table by expand the scan startPosition, we need to pick first values from the startPosition, if startPosition
         * does not cover all the key columns, we can pick the values for the remaining consecutive key columns from firstHashRow
         */
        if (nCols == columnOrdering.length) {
            // the current scan startkey has covered the whole key column set, no need to expand further
            scanStartOverride = startPosition;
        } else {
            // this is 0-based array, the column positions in the array are also 0-based.
            int[] colToBaseTableMap = ((MergeJoinOperation)joinOperation).getRightHashKeyToBaseTableMap();

            // we cannot map the hash fields back to the base table column position, so cannot utilize the hash field value
            // to restrict the rigth table scan
            if (colToBaseTableMap == null)
                scanStartOverride = startPosition;
            else {

                ExecRow newStartKey = new ValueRow(columnOrdering.length);
                int[] rightHashKeys = joinOperation.getRightHashKeys();

                for (int i = 0; i < nCols; i++)
                    newStartKey.setColumn(i + 1, startPosition.getColumn(i + 1));

                int len = nCols;
                for (int i = len; i < columnOrdering.length; i++) {
                    int keyColumnBaseTablePosition = columnOrdering[i];
                    int j = 0;
                    for (; j < rightHashKeys.length; j++) {
                        // both columnOrdering and colToBaseTableMap are 0-based
                        if (colToBaseTableMap[j] == keyColumnBaseTablePosition) {
                            newStartKey.setColumn(i + 1, firstHashRow.getColumn(j + 1));
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
        }

        ((BaseActivation)joinOperation.getActivation()).setScanStartOverride(scanStartOverride);

        // we can only set the stop key if start key is from equality predicate like "key=constant"
        if (startPosition != null) {
            ScanInformation<ExecRow>  scanInfo = joinOperation.getRightResultSet().getScanInformation();
            if (scanInfo != null && scanInfo.getSameStartStopPosition())
                ((BaseActivation)joinOperation.getActivation()).setScanStopOverride(startPosition);
        }
    }


    protected abstract AbstractMergeJoinIterator createMergeJoinIterator(PeekingIterator<ExecRow> leftPeekingIterator,
                                                                         PeekingIterator<ExecRow> rightPeekingIterator,
                                                                         int[] leftHashKeys, int[] rightHashKeys, JoinOperation joinOperation, OperationContext<JoinOperation> operationContext);
}
