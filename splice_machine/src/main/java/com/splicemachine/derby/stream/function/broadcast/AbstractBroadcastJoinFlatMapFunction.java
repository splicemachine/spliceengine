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

package com.splicemachine.derby.stream.function.broadcast;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.ControlExecutionLimiter;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.ResubmitDistributedException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.TriggerExecutionContext;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.JoinTable;
import com.splicemachine.derby.impl.sql.execute.operations.*;
import com.splicemachine.derby.stream.function.InnerJoinNullFilterFunction;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.stream.Stream;
import com.splicemachine.stream.Streams;
import splice.com.google.common.base.Function;
import splice.com.google.common.collect.FluentIterable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.splicemachine.db.impl.sql.execute.TriggerExecutionContext.pushTriggerExecutionContextFromActivation;

/**
 * Created by dgomezferro on 11/4/15.
 */
public abstract class AbstractBroadcastJoinFlatMapFunction<In, Out> extends SpliceFlatMapFunction<JoinOperation, Iterator<In>, Out> {
    private static final BroadcastJoinCache broadcastJoinCache = new BroadcastJoinCache();
    private JoinOperation operation;
    private Future<JoinTable> joinTable ;
    private boolean init = false;
    protected boolean rightAsLeft;
    protected ContextManager cm;
    protected boolean newContextManager, lccPushed;
    private boolean noCacheBroadcastJoinRight;

    public AbstractBroadcastJoinFlatMapFunction() {
    }

    public AbstractBroadcastJoinFlatMapFunction(OperationContext operationContext,
                                                boolean noCacheBroadcastJoinRight) {
        super(operationContext);
        this.rightAsLeft = false;
        this.noCacheBroadcastJoinRight = noCacheBroadcastJoinRight;
    }

    public AbstractBroadcastJoinFlatMapFunction(OperationContext operationContext,
                                                boolean rightAsLeft,
                                                boolean noCacheBroadcastJoinRight) {
        super(operationContext);
        this.rightAsLeft = rightAsLeft;
        this.noCacheBroadcastJoinRight = noCacheBroadcastJoinRight;
    }
    @Override
    public final Iterator<Out> call(Iterator<In> locatedRows) throws Exception {
        init();
        JoinTable table;
        try {
            table = joinTable.get();
        } catch (ExecutionException ee) {
            Throwable c = ee.getCause();
            if (c instanceof ResubmitDistributedException) {
                throw (ResubmitDistributedException) c;
            }
            throw ee;
        }
        Iterator<Out> it = call(locatedRows, table).iterator();
        return new Iterator<Out>() {
            @Override
            public boolean hasNext() {
                boolean result = it.hasNext();
                if (!result) {
                    table.close();
                    joinTable = null; // delete reference for gc
                    cleanupLCCInContext();
                }
                return result;
            }

            @Override
            public Out next() {
                return it.next();
            }
        };
    }

    protected abstract Iterable<Out> call(Iterator<In> locatedRows, JoinTable joinTable);

    protected void cleanupLCCInContext() {
        if (cm != null) {
            if (lccPushed)
                cm.popContext();
            if (newContextManager)
                ContextService.getFactory().resetCurrentContextManager(cm);
            cm = null;
        }
        newContextManager = false;
        lccPushed = false;
    }

    private boolean needsLCCInContext() {
        if (operationContext != null) {
            Activation activation = operationContext.getActivation();
            if (activation != null) {
                LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
                if (lcc != null) {
                    TriggerExecutionContext tec = lcc.getTriggerExecutionContext();
                    if (tec != null)
                        return tec.currentTriggerHasReferencingClause();
                }
            }
        }
        return false;
    }

    // Push the LanguageConnectionContext to the current Context Manager
    // if we're executing a trigger with a referencing clause.
    private void initCurrentLCC() throws StandardException {
        if (!needsLCCInContext())
            return;
        cm = ContextService.getFactory().getCurrentContextManager();
        if (cm == null) {
            newContextManager = true;
            cm = ContextService.getFactory().newContextManager();
            ContextService.getFactory().setCurrentContextManager(cm);
        }
        if (cm != null) {
            if (operationContext != null)
                lccPushed = pushTriggerExecutionContextFromActivation(operationContext.getActivation(), cm);
        }
    }

    private synchronized void init() throws Exception {
        if (init)
            return;
        init = true;
        joinTable = SIDriver.driver().getExecutorService().submit(() -> {
            initCurrentLCC();
            operation = getOperation();
            ControlExecutionLimiter limiter = operation.getActivation().getLanguageConnectionContext().getControlExecutionLimiter();
            SpliceOperation rightOperation, leftOperation;
            int[] rightHashKeys, leftHashKeys;
            long sequenceId;
            if (rightAsLeft) {
                // switch left and right source
                rightOperation = operation.getLeftOperation();
                leftOperation = operation.getRightOperation();
                rightHashKeys = operation.getLeftHashKeys();
                leftHashKeys = operation.getRightHashKeys();
                sequenceId = operation.getLeftSequenceId();

            } else {
                rightOperation = operation.getRightOperation();
                leftOperation = operation.getLeftOperation();
                rightHashKeys = operation.getRightHashKeys();
                leftHashKeys = operation.getLeftHashKeys();
                sequenceId = operation.getRightSequenceId();
            }

            Callable<Stream<ExecRow>> rhsLoader = () -> {
                DataSetProcessorFactory dataSetProcessorFactory=EngineDriver.driver().processorFactory();

                final DataSetProcessor dsp =
                        (rightOperation instanceof MultiProbeTableScanOperation &&
                         rightOperation.getEstimatedRowCount() <
                         operation.getActivation().getLanguageConnectionContext().
                                                   getOptimizerFactory().getDetermineSparkRowThreshold()) ?
                       dataSetProcessorFactory.localProcessor(getActivation(), rightOperation) :
                       dataSetProcessorFactory.bulkProcessor(getActivation(), rightOperation);

                return Streams.wrap(FluentIterable.from(() -> {
                    try{
                        operation.reset();
                        DataSet<ExecRow> rightDataSet = rightOperation.getDataSet(dsp);
                        if (rightHashKeys.length != 0)
                            rightDataSet = rightDataSet.filter(new InnerJoinNullFilterFunction(operationContext,rightHashKeys));
                        return rightDataSet.toLocalIterator();
                    }catch(StandardException e){
                        throw new RuntimeException(e);
                    }
                }).transform(new Function<ExecRow, ExecRow>() {
                    @Nullable
                    @Override
                    public ExecRow apply(@Nullable ExecRow locatedRow) {
                        assert locatedRow!=null;
                        limiter.addAccumulatedRows(1);
                        operationContext.recordJoinedRight();
                        return locatedRow;
                    }
                }));
            };
            ExecRow leftTemplate = leftOperation.getExecRowDefinition();

            if (noCacheBroadcastJoinRight) {
                BroadcastJoinNoCacheLoader loader = new BroadcastJoinNoCacheLoader(sequenceId, rightHashKeys, leftHashKeys, leftTemplate, rhsLoader);
                return loader.call().newTable();
            }

            return broadcastJoinCache.get(sequenceId, rhsLoader, rightHashKeys, leftHashKeys, leftTemplate).newTable();
        });
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(rightAsLeft);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        rightAsLeft = in.readBoolean();
    }
}
