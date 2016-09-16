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

package com.splicemachine.derby.stream.function.broadcast;

import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.stream.function.InnerJoinNullFilterFunction;
import org.spark_project.guava.base.Function;
import org.spark_project.guava.collect.FluentIterable;
import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.impl.sql.JoinTable;
import com.splicemachine.derby.impl.sql.execute.operations.BroadcastJoinCache;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.stream.Stream;
import com.splicemachine.stream.Streams;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.concurrent.Callable;

/**
 * Created by dgomezferro on 11/4/15.
 */
public abstract class AbstractBroadcastJoinFlatMapFunction<In, Out> extends SpliceFlatMapFunction<JoinOperation, Iterator<In>, Out> {
    private static final BroadcastJoinCache broadcastJoinCache = new BroadcastJoinCache();
    private JoinOperation operation;

    public AbstractBroadcastJoinFlatMapFunction() {
    }

    public AbstractBroadcastJoinFlatMapFunction(OperationContext operationContext) {
        super(operationContext);
    }

    @Override
    public final Iterator<Out> call(Iterator<In> locatedRows) throws Exception {
        JoinTable joinTable ;
        operation = getOperation();
        Callable<Stream<ExecRow>> rhsLoader = new Callable<Stream<ExecRow>>() {
            @Override
            public Stream<ExecRow> call() throws Exception {
                DataSetProcessorFactory dataSetProcessorFactory=EngineDriver.driver().processorFactory();
                final DataSetProcessor dsp =dataSetProcessorFactory.bulkProcessor(getActivation(),operation.getRightOperation());
                return Streams.wrap(FluentIterable.from(new Iterable<LocatedRow>(){
                    @Override
                    public Iterator<LocatedRow> iterator(){
                        try{
                            return operation.getRightOperation().getDataSet(dsp).filter(new InnerJoinNullFilterFunction(operationContext,operation.getRightHashKeys())).toLocalIterator();
                        }catch(StandardException e){
                            throw new RuntimeException(e);
                        }
                    }
                }).transform(new Function<LocatedRow, ExecRow>() {
                    @Nullable
                    @Override
                    public ExecRow apply(@Nullable LocatedRow locatedRow) {
                        assert locatedRow!=null;
                        operationContext.recordJoinedRight();
                        return locatedRow.getRow();
                    }
                }));
            }
        };
        ExecRow leftTemplate = operation.getLeftOperation().getExecRowDefinition();
        joinTable = broadcastJoinCache.get(operation.getSequenceId(), rhsLoader, operation.getRightHashKeys(), operation.getLeftHashKeys(), leftTemplate).newTable();

        return call(locatedRows, joinTable).iterator();
    }

    protected abstract Iterable<Out> call(Iterator<In> locatedRows, JoinTable joinTable);
}
