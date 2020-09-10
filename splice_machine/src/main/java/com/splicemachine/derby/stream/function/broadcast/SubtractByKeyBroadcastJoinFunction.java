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
import splice.com.google.common.base.Predicate;
import splice.com.google.common.collect.FluentIterable;
import com.splicemachine.derby.impl.sql.JoinTable;
import com.splicemachine.derby.stream.iapi.OperationContext;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * Created by dgomezferro on 11/6/15.
 */
public class SubtractByKeyBroadcastJoinFunction extends AbstractBroadcastJoinFlatMapFunction<ExecRow, ExecRow> {
    public SubtractByKeyBroadcastJoinFunction() {
    }

    public SubtractByKeyBroadcastJoinFunction(OperationContext operationContext,
                                              boolean noCacheBroadcastJoinRight) {
        super(operationContext, noCacheBroadcastJoinRight);
    }

    @Override
    protected Iterable<ExecRow> call(final Iterator<ExecRow> locatedRows, final JoinTable joinTable) {
        return FluentIterable.from(new Iterable<ExecRow>(){
            @Override
            public Iterator<ExecRow> iterator(){
                return locatedRows;
            }
        }).filter(new Predicate<ExecRow>() {
            @Override
            public boolean apply(@Nullable ExecRow locatedRow) {
                try {
                    boolean rowsOnRight = joinTable.fetchInner(locatedRow).hasNext();
                    return !rowsOnRight;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
