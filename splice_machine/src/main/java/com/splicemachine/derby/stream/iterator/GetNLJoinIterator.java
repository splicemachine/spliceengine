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
