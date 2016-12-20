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

import org.spark_project.guava.base.Function;
import org.spark_project.guava.collect.FluentIterable;
import org.spark_project.guava.collect.Iterables;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.JoinTable;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * Created by dgomezferro on 11/4/15.
 */
public class BroadcastJoinFlatMapFunction extends AbstractBroadcastJoinFlatMapFunction<LocatedRow, Tuple2<ExecRow, Tuple2<LocatedRow, LocatedRow>>> {

    public BroadcastJoinFlatMapFunction() {
    }

    public BroadcastJoinFlatMapFunction(OperationContext operationContext) {
        super(operationContext);
    }

    @Override
    public Iterable<Tuple2<ExecRow, Tuple2<LocatedRow, LocatedRow>>> call(final Iterator<LocatedRow> locatedRows, final JoinTable joinTable) {
        return Iterables.concat(FluentIterable.from(new Iterable<LocatedRow>(){
            @Override
            public Iterator<LocatedRow> iterator(){
                return locatedRows;
            }
        }).transform(
                new Function<LocatedRow, Iterable<Tuple2<ExecRow, Tuple2<LocatedRow, LocatedRow>>>>(){
                    @Nullable
                    @Override
                    public Iterable<Tuple2<ExecRow, Tuple2<LocatedRow, LocatedRow>>> apply(@Nullable final LocatedRow left){
                        Iterable<ExecRow> inner=new Iterable<ExecRow>(){
                            @Override
                            public Iterator<ExecRow> iterator(){
                                try{
                                    return joinTable.fetchInner(left.getRow());
                                }catch(Exception e){
                                    throw new RuntimeException(e);
                                }
                            }
                        };
                        return FluentIterable.from(inner).transform(
                                new Function<ExecRow, Tuple2<ExecRow, Tuple2<LocatedRow, LocatedRow>>>(){
                                    @Nullable
                                    @Override
                                    public Tuple2<ExecRow, Tuple2<LocatedRow, LocatedRow>> apply(@Nullable ExecRow right){
                                        return new Tuple2<>(left.getRow(),new Tuple2<>(left,new LocatedRow(right)));
                                    }
                                });
                    }
                }));
    }
}
