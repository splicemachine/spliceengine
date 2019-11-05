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

package com.splicemachine.derby.stream.function.broadcast;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.ControlExecutionLimiter;
import com.splicemachine.db.iapi.sql.conn.ResubmitDistributedException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.JoinTable;
import com.splicemachine.derby.impl.sql.execute.operations.BroadcastJoinCache;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.MultiProbeTableScanOperation;
import com.splicemachine.derby.stream.function.InnerJoinNullFilterFunction;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.stream.Stream;
import com.splicemachine.stream.Streams;
import org.spark_project.guava.base.Function;
import org.spark_project.guava.collect.FluentIterable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by dgomezferro on 11/4/15.
 */
public abstract class AbstractBroadcastJoinFlatMapFunction<In, Out> extends SpliceFlatMapFunction<JoinOperation, Iterator<In>, Out> {
    private static final BroadcastJoinCache broadcastJoinCache = new BroadcastJoinCache();
    private JoinOperation operation;
    private Future<JoinTable> joinTable ;
    private boolean init = false;
    protected boolean rightAsLeft;

    public AbstractBroadcastJoinFlatMapFunction() {
    }

    public AbstractBroadcastJoinFlatMapFunction(OperationContext operationContext) {
        super(operationContext);
        this.rightAsLeft = false;
    }

    public AbstractBroadcastJoinFlatMapFunction(OperationContext operationContext, boolean rightAsLeft) {
        super(operationContext);
        this.rightAsLeft = rightAsLeft;
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

    private synchronized void init() {
        if (init)
            return;
        init = true;
        joinTable = SIDriver.driver().getExecutorService().submit(() -> {
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
