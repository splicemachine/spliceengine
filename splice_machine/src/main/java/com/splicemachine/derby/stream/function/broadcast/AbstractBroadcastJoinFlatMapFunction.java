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
