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

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.JoinTable;
import com.splicemachine.derby.stream.function.IteratorUtils;
import com.splicemachine.derby.stream.iapi.OperationContext;
import splice.com.google.common.base.Function;
import splice.com.google.common.collect.FluentIterable;
import splice.com.google.common.collect.Iterables;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * Created by dgomezferro on 11/4/15.
 */
public class BroadcastJoinFlatMapFunction extends AbstractBroadcastJoinFlatMapFunction<ExecRow, Tuple2<ExecRow, Tuple2<ExecRow, ExecRow>>> {

    public BroadcastJoinFlatMapFunction() {
    }

    public BroadcastJoinFlatMapFunction(OperationContext operationContext,
                                        boolean noCacheBroadcastJoinRight) {
        super(operationContext, noCacheBroadcastJoinRight);
    }

    @Override
    public Iterable<Tuple2<ExecRow, Tuple2<ExecRow, ExecRow>>> call(final Iterator<ExecRow> locatedRows, final JoinTable joinTable) {
        return Iterables.concat(FluentIterable.from(new Iterable<ExecRow>(){
            @Override
            public Iterator<ExecRow> iterator(){
                return locatedRows;
            }
        }).transform(
                new Function<ExecRow, Iterable<Tuple2<ExecRow, Tuple2<ExecRow, ExecRow>>>>(){
                    @Nullable
                    @Override
                    public Iterable<Tuple2<ExecRow, Tuple2<ExecRow, ExecRow>>> apply(@Nullable final ExecRow left){
                        Iterable<ExecRow> inner=new Iterable<ExecRow>(){
                            @Override
                            public Iterator<ExecRow> iterator(){
                                try{
                                    return IteratorUtils.asInterruptibleIterator(joinTable.fetchInner(left));
                                }catch(Exception e){
                                    throw new RuntimeException(e);
                                }
                            }
                        };
                        return FluentIterable.from(inner).transform(
                                new Function<ExecRow, Tuple2<ExecRow, Tuple2<ExecRow, ExecRow>>>(){
                                    @Nullable
                                    @Override
                                    public Tuple2<ExecRow, Tuple2<ExecRow, ExecRow>> apply(@Nullable ExecRow right){
                                        return new Tuple2<>(left,new Tuple2<>(left,right));
                                    }
                                });
                    }
                }));
    }
}
