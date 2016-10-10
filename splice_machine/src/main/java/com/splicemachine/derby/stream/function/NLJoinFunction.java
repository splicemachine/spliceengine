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

package com.splicemachine.derby.stream.function;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.IterableJoinFunction;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iterator.GetNLJoinIterator;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.utils.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by jyuan on 10/10/16.
 */
public abstract class NLJoinFunction <Op extends SpliceOperation, From, To> extends SpliceJoinFlatMapFunction<Op, From, To> implements IterableJoinFunction {

    public enum JoinType{
        INNER,
        LEFT_OUTER,
        ANTI,
        ONE_ROW_INNER
    }
    protected JoinType joinType;
    protected boolean initialized;
    protected int batchSize;
    protected Iterator<LocatedRow> leftSideIterator;
    protected List<OperationContext> operationContextList;
    protected int nLeftRows;
    protected OperationContext currentOperationContext;
    protected Iterator<LocatedRow> rightSideNLJIterator;
    protected ExecRow leftRow;
    protected RowLocation leftRowLocation;
    protected boolean isLeftOuterJoin;
    protected boolean isAntiJoin;
    protected boolean isOneRowInnerJoin;

    protected ExecutorCompletionService<Pair<OperationContext, Iterator<LocatedRow>>> completionService;
    protected ExecutorService executorService;

    public NLJoinFunction () {}

    public NLJoinFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }


    protected void init(Iterator<LocatedRow> from) throws StandardException {
        checkInit();
        SConfiguration configuration= EngineDriver.driver().getConfiguration();
        batchSize = configuration.getNestedLoopJoinBatchSize();
        nLeftRows = 0;
        leftSideIterator = from;
        executorService = Executors.newFixedThreadPool(batchSize);
        completionService = new ExecutorCompletionService<>(executorService);

        initOperationContexts();
        loadBatch();
    }

    private void initOperationContexts() throws StandardException {
        try {
            operationContextList = new ArrayList<>(batchSize);
            for (int i = 0; i < batchSize; ++i) {
                operationContextList.add(operationContext.getClone());
            }
        }
        catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    }

    private void loadBatch() throws StandardException {

        try {
            while (nLeftRows < batchSize) {
                if (!leftSideIterator.hasNext())
                    break;
                nLeftRows++;
                LocatedRow locatedRow = leftSideIterator.next();
                GetNLJoinIterator getNLJoinIterator =  GetNLJoinIterator.makeGetNLJoinIterator(joinType,
                        operationContextList.remove(0), locatedRow);
                completionService.submit(getNLJoinIterator);
            }
            if (nLeftRows > 0) {
                Future<Pair<OperationContext, Iterator<LocatedRow>>> future = completionService.take();
                Pair<OperationContext, Iterator<LocatedRow>> result = future.get();
                currentOperationContext = result.getFirst();
                rightSideNLJIterator = result.getSecond();
                leftRow = currentOperationContext.getOperation().getLeftOperation().getCurrentRow();
                leftRowLocation = currentOperationContext.getOperation().getLeftOperation().getCurrentRowLocation();
                operationContext.getOperation().getLeftOperation().setCurrentLocatedRow(getLeftLocatedRow());
                nLeftRows--;
            }
        }
        catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public boolean hasNext() {

        try {
            if (rightSideNLJIterator == null)
                return false;
            while (nLeftRows > 0 && !rightSideNLJIterator.hasNext()) {

                // We have consumed all rows from right side iterator, reclaim operation context
                currentOperationContext.getOperation().close();
                operationContextList.add(currentOperationContext);

                if (leftSideIterator.hasNext()) {
                    // If we haven't consumed left side iterator, submit a task to scan righ side
                    LocatedRow locatedRow = leftSideIterator.next();
                    GetNLJoinIterator getNLJoinIterator = GetNLJoinIterator.makeGetNLJoinIterator(joinType,
                            operationContextList.remove(0), locatedRow);
                    completionService.submit(getNLJoinIterator);
                    nLeftRows++;
                }

                if(nLeftRows > 0) {
                    // If there are pending tasks, wait to get an iterator to righ side
                    Future<Pair<OperationContext, Iterator<LocatedRow>>> future = completionService.take();
                    Pair<OperationContext, Iterator<LocatedRow>> result = future.get();
                    nLeftRows--;
                    currentOperationContext = result.getFirst();
                    rightSideNLJIterator = result.getSecond();
                    leftRow = currentOperationContext.getOperation().getLeftOperation().getCurrentRow();
                    leftRowLocation = currentOperationContext.getOperation().getLeftOperation().getCurrentRowLocation();
                    operationContext.getOperation().getLeftOperation().setCurrentLocatedRow(getLeftLocatedRow());
                }
            }
            if (!rightSideNLJIterator.hasNext()) {
                executorService.shutdownNow();
            }

            return rightSideNLJIterator.hasNext();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public LocatedRow getLeftLocatedRow() {
        return new LocatedRow(leftRowLocation, leftRow);
    }

    @Override
    public boolean wasRightOuterJoin() {
        return op.wasRightOuterJoin;
    }

    @Override
    public ExecRow getLeftRow() {
        return leftRow;
    }

    @Override
    public RowLocation getLeftRowLocation() {
        return leftRowLocation;
    }

    @Override
    public void setCurrentLocatedRow(LocatedRow locatedRow) {
        op.setCurrentLocatedRow(locatedRow);
    }

    @Override
    public int getResultSetNumber() {
        return op.resultSetNumber();
    }

    @Override
    public OperationContext getOperationContext() {
        return operationContext;
    }

    @Override
    public ExecutionFactory getExecutionFactory() {
        return executionFactory;
    }

    @Override
    public int getNumberOfColumns() {
        return numberOfColumns;
    }

    @Override
    public ExecRow getRightRow() {
        ExecRow row = rightSideNLJIterator.next().getRow();
        SpliceOperation from = currentOperationContext.getOperation().getRightOperation();
        SpliceOperation to = operationContext.getOperation().getRightOperation();
        setRightSideCurrentLocatedRow(from, to);
        return row;
    }

    private void setRightSideCurrentLocatedRow(SpliceOperation from, SpliceOperation to) {
        try {
            try {
                ExecRow row = from.getCurrentRow();
                RowLocation location = from.getCurrentRowLocation();
                to.setCurrentRow(row);
                to.setCurrentRowLocation(location);
            }
            catch (Exception e) {
                return;
            }
            List<SpliceOperation> fromSubOperations = from.getSubOperations();
            List<SpliceOperation> toSubOperations = to.getSubOperations();
            for (int i = 0; i < fromSubOperations.size(); ++i) {
                setRightSideCurrentLocatedRow(fromSubOperations.get(i), toSubOperations.get(i));
            }
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
