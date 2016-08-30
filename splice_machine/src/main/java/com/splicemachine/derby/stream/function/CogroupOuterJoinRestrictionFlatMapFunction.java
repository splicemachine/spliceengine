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

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.utils.ConcatenatedIterable;
import scala.Tuple2;

import java.util.*;

/**
 *
 * Created by jleach on 4/30/15.
 */
public class CogroupOuterJoinRestrictionFlatMapFunction<Op extends SpliceOperation> extends SpliceJoinFlatMapFunction<Op, Tuple2<ExecRow,Tuple2<Iterable<LocatedRow>,Iterable<LocatedRow>>>,LocatedRow> {
    private OuterJoinRestrictionFlatMapFunction<Op> outerJoinRestrictionFlatMapFunction;
    protected LocatedRow leftRow;
    public CogroupOuterJoinRestrictionFlatMapFunction() {
        super();
    }

    public CogroupOuterJoinRestrictionFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public Iterable<LocatedRow> call(Tuple2<ExecRow,Tuple2<Iterable<LocatedRow>, Iterable<LocatedRow>>> tuple) throws Exception {
        checkInit();
        //linked list saves memory, and since we are just doing iteration anyway, there isn't really much penalty here
        List<Iterable<LocatedRow>> returnRows = new LinkedList<>();
        for(LocatedRow a_1 : tuple._2._1){
            Iterable<LocatedRow> locatedRows=tuple._2._2;
            returnRows.add(outerJoinRestrictionFlatMapFunction.call(new Tuple2<>(a_1,locatedRows)));
        }
        return new ConcatenatedIterable<>(returnRows);
    }
    @Override
    protected void checkInit() {
        if (!initialized)
            outerJoinRestrictionFlatMapFunction = new OuterJoinRestrictionFlatMapFunction<>(operationContext);
        super.checkInit();
    }

}