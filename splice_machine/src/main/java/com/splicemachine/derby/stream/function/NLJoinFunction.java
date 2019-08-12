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

package com.splicemachine.derby.stream.function;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.ExceptionUtil;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.HBaseRowLocation;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.IterableJoinFunction;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iterator.GetNLJoinIterator;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.Pair;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskKilledException;
import org.apache.spark.util.TaskCompletionListener;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Supplier;

 /**
 * Created by jyuan on 10/10/16.
 */
public abstract class NLJoinFunction <Op extends SpliceOperation, From, To> extends SpliceJoinFlatMapFunction<Op, From, To>
        implements IterableJoinFunction, AutoCloseable {
    private static final Logger LOG = Logger.getLogger(NLJoinFunction.class);

    public enum JoinType{
        INNER,
        LEFT_OUTER,
        ANTI,
        ONE_ROW_INNER
    }
    protected JoinType joinType;
    protected boolean initialized;
    protected int batchSize;
    protected Iterator<ExecRow> leftSideIterator;
    protected List<OperationContext> operationContextList;
    protected int nLeftRows;
    protected OperationContext currentOperationContext;
    protected Iterator<ExecRow> rightSideNLJIterator;
    protected ExecRow leftRow;
    protected RowLocation leftRowLocation;
    protected boolean isLeftOuterJoin;
    protected boolean isAntiJoin;
    protected boolean isOneRowInnerJoin;
    protected boolean hasMatch;
    protected List<Future<Pair<OperationContext, Iterator<ExecRow>>>> futures;
    protected Set<OperationContext> allContexts;
    protected TaskContext taskContext;
    private Deque<ExecRow> firstBatch;
    private volatile boolean isClosed = false;

    protected ExecutorService executorService;

    public NLJoinFunction () {}

    public NLJoinFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }


    protected void init(Iterator<ExecRow> from) throws StandardException {
        checkInit();
        taskContext = TaskContext.get();
        if (taskContext != null) {
            taskContext.addTaskCompletionListener((TaskCompletionListener) (t) -> close());
        }
        operationContext.getOperation().registerCloseable(this);
        SConfiguration configuration= EngineDriver.driver().getConfiguration();
        batchSize = configuration.getNestedLoopJoinBatchSize();
        nLeftRows = 0;
        leftSideIterator = from;
        executorService = SIDriver.driver().getExecutorService();
        firstBatch = new ArrayDeque<>(batchSize);

        initOperationContexts();
        loadBatch();
    }



    private void initOperationContexts() throws StandardException {
        try {
            operationContextList = new ArrayList<>(batchSize);
            allContexts = Collections.synchronizedSet(new HashSet<>());
            for (int i = 0; i < batchSize && leftSideIterator.hasNext(); ++i) {
                firstBatch.addLast(leftSideIterator.next().getClone());
            }
        }
        catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void close() {
        StandardException se = null;
        synchronized (allContexts) {
            isClosed = true;
            for (OperationContext ctx : allContexts) {
                try {
                    ctx.getOperation().close();
                } catch (StandardException e) {
                    se = e;
                    LOG.error("Exception while closing operation", e);
                }
            }
        }
        if (futures != null) {
            for (Future<Pair<OperationContext, Iterator<ExecRow>>> future : futures) {
                future.cancel(true);
            }
        }
        if (se != null)
            Exceptions.throwAsRuntime(se);
    }

    private void loadBatch() throws StandardException {

        try {
            futures = new ArrayList<Future<Pair<OperationContext, Iterator<ExecRow>>>>();
            while (nLeftRows < batchSize) {
                if (firstBatch.isEmpty())
                    break;
                nLeftRows++;
                ExecRow execRow = firstBatch.removeFirst();
                OperationContext context = operationContextList.isEmpty() ? null : operationContextList.remove(0);
                Supplier<OperationContext> supplier = context != null ? () -> context : () -> {
                    try {
                        OperationContext ctx = operationContext.getClone();
                        allContexts.add(ctx);
                        if(isClosed)
                            ctx.getOperation().close();
                        return ctx;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };
                GetNLJoinIterator getNLJoinIterator =  GetNLJoinIterator.makeGetNLJoinIterator(joinType,
                        supplier, execRow);
                futures.add(executorService.submit(getNLJoinIterator));
            }
            if (nLeftRows > 0) {
                Future<Pair<OperationContext, Iterator<ExecRow>>> future = futures.remove(0);
                Pair<OperationContext, Iterator<ExecRow>> result = future.get();
                currentOperationContext = result.getFirst();
                rightSideNLJIterator = result.getSecond();
                leftRow = currentOperationContext.getOperation().getLeftOperation().getCurrentRow();
                leftRowLocation = new HBaseRowLocation(leftRow.getKey());
                operationContext.getOperation().getLeftOperation().setCurrentRow(getLeftLocatedRow());
                operationContext.getOperation().getLeftOperation().setCurrentRowLocation(getLeftRowLocation());
                hasMatch = false;
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
            while (true) {
                while (nLeftRows > 0 && !rightSideNLJIterator.hasNext()) {

                    // We have consumed all rows from right side iterator, reclaim operation context
                    currentOperationContext.getOperation().close();
                    operationContextList.add(currentOperationContext);

                    if (leftSideIterator.hasNext()) {
                        // If we haven't consumed left side iterator, submit a task to scan righ side
                        ExecRow execRow = leftSideIterator.next();
                        OperationContext ctx = operationContextList.remove(0);
                        GetNLJoinIterator getNLJoinIterator = GetNLJoinIterator.makeGetNLJoinIterator(joinType,
                                () -> ctx, execRow.getClone());
                        futures.add(executorService.submit(getNLJoinIterator));
                        nLeftRows++;
                    }

                    if (nLeftRows > 0) {
                        // If there are pending tasks, wait to get an iterator to righ side
                        Future<Pair<OperationContext, Iterator<ExecRow>>> future = futures.remove(0);
                        if (taskContext != null && taskContext.isInterrupted()) {
                            LOG.warn("Task killed, raising exception!");
                            throw new TaskKilledException();
                        }
                        Pair<OperationContext, Iterator<ExecRow>> result;
                        while (true) {
                            try {
                                result = future.get(1, TimeUnit.SECONDS);
                                break;
                            } catch (TimeoutException e) {
                                if (taskContext != null && taskContext.isInterrupted()) {
                                    LOG.warn("Task killed, raising exception!");
                                    throw new TaskKilledException();
                                }
                            }
                        }
                        nLeftRows--;
                        currentOperationContext = result.getFirst();
                        rightSideNLJIterator = result.getSecond();
                        leftRow = currentOperationContext.getOperation().getLeftOperation().getCurrentRow();
                        leftRowLocation = new HBaseRowLocation(leftRow.getKey());
                        operationContext.getOperation().getLeftOperation().setCurrentRow(getLeftLocatedRow());
                        operationContext.getOperation().getLeftOperation().setCurrentRowLocation(getLeftRowLocation());
                        hasMatch = false;
                    }
                }

                boolean result = rightSideNLJIterator.hasNext();
                if (result) {
                    if (!hasMatch)
                        hasMatch = true;
                    else {
                        if (isSemiJoin) {
                            //skip the subsequent rows
                            rightSideNLJIterator.next();
                            continue;
                        }
                        // for SSQ, if we get a second matching row for the same left row, report error
                        if (forSSQ)
                            throw StandardException.newException(SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION);
                    }
                }
                return result;
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ExecRow getLeftLocatedRow() {
        return leftRow;
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
    public void setCurrentLocatedRow(ExecRow locatedRow) {
        op.setCurrentRow(locatedRow);
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
        ExecRow row = rightSideNLJIterator.next();
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
