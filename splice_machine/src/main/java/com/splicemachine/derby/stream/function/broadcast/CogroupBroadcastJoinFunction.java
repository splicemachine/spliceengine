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

import org.spark_project.guava.base.Function;
import org.spark_project.guava.collect.FluentIterable;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.JoinTable;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * Created by dgomezferro on 11/6/15.
 */
public class CogroupBroadcastJoinFunction extends AbstractBroadcastJoinFlatMapFunction<LocatedRow, Tuple2<LocatedRow,Iterable<LocatedRow>>> {
    public CogroupBroadcastJoinFunction() {
    }

    public CogroupBroadcastJoinFunction(OperationContext operationContext) {
        super(operationContext);
    }

    @Override
    protected Iterable<Tuple2<LocatedRow, Iterable<LocatedRow>>> call(final Iterator<LocatedRow> locatedRows, final JoinTable joinTable) {
        return FluentIterable.from(new Iterable<LocatedRow>() {
            @Override
            public Iterator<LocatedRow> iterator() {
                return locatedRows;
            }
        }).transform(
                new Function<LocatedRow, Tuple2<LocatedRow, Iterable<LocatedRow>>>() {
                    @Nullable
                    @Override
                    public Tuple2<LocatedRow, Iterable<LocatedRow>> apply(@Nullable final LocatedRow left) {
                        FluentIterable<LocatedRow> inner = FluentIterable.from(new Iterable<ExecRow>(){
                            @Override
                            public Iterator<ExecRow> iterator(){
                                try{
                                    return joinTable.fetchInner(left.getRow());
                                }catch(Exception e){
                                    throw new RuntimeException(e);
                                }
                            }
                        }).transform(new Function<ExecRow, LocatedRow>() {
                            @Nullable
                            @Override
                            public LocatedRow apply(@Nullable ExecRow execRow) {
                                return new LocatedRow(execRow);
                            }
                        });
                        return new Tuple2<LocatedRow,Iterable<LocatedRow>>(left, inner);
                    }
                });
    }
}
