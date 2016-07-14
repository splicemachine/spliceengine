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

import org.sparkproject.guava.collect.Iterables;
import org.sparkproject.guava.collect.Sets;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by jleach on 4/30/15.
 */
public class CogroupOuterJoinRestrictionFlatMapFunction<Op extends SpliceOperation> extends SpliceJoinFlatMapFunction<Op, Tuple2<ExecRow,Tuple2<Iterable<LocatedRow>,Iterable<LocatedRow>>>,LocatedRow> {
    protected OuterJoinRestrictionFlatMapFunction outerJoinRestrictionFlatMapFunction;
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
//        Set<LocatedRow> rightSide = Sets.newHashSet(tuple._2._2); // Memory Issue, HashSet ?
        Iterable<LocatedRow> returnRows = new ArrayList<>();
        for(Object a_1 : tuple._2._1){
            returnRows=Iterables.concat(returnRows,outerJoinRestrictionFlatMapFunction.call(new Tuple2<>(a_1,tuple._2._2)));
        }
        return returnRows;
    }
    @Override
    protected void checkInit() {
        if (!initialized)
            outerJoinRestrictionFlatMapFunction = new OuterJoinRestrictionFlatMapFunction<>(operationContext);
        super.checkInit();
    }

}