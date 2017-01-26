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

package com.splicemachine.derby.stream.iterator;

import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.NLJoinFunction;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.utils.Pair;

import java.util.Iterator;
import java.util.concurrent.Callable;

/**
 * Created by jyuan on 10/10/16.
 */
public abstract class GetNLJoinIterator implements Callable<Pair<OperationContext, Iterator<LocatedRow>>> {

    protected LocatedRow locatedRow;
    protected OperationContext operationContext;

    public GetNLJoinIterator() {}

    public GetNLJoinIterator(OperationContext operationContext, LocatedRow locatedRow) {
        this.operationContext = operationContext;
        this.locatedRow = locatedRow;
    }

    public abstract Pair<OperationContext, Iterator<LocatedRow>> call() throws Exception;



    public static GetNLJoinIterator makeGetNLJoinIterator(NLJoinFunction.JoinType joinType,
                                                   OperationContext operationContext,
                                                   LocatedRow locatedRow) {
        switch (joinType) {
            case INNER:
                return new GetNLJoinInnerIterator(operationContext, locatedRow);

            case LEFT_OUTER:
                return new GetNLJoinLeftOuterIterator(operationContext, locatedRow);

            case ANTI:
                return new GetNLJoinAntiIterator(operationContext, locatedRow);

            case ONE_ROW_INNER:
                return new GetNLJoinOneRowIterator(operationContext, locatedRow);

            default:
                throw new RuntimeException("Unrecognized nested loop join type");
        }
    }
}
