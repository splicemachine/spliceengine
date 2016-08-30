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

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.utils.ConcatenatedIterable;
import org.apache.commons.collections.IteratorUtils;
import scala.Tuple2;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * Created by jleach on 4/30/15.
 */
public class CogroupInnerJoinRestrictionFlatMapFunction<Op extends SpliceOperation> extends SpliceJoinFlatMapFunction<Op, Tuple2<Iterable<LocatedRow>,Iterable<LocatedRow>>,LocatedRow> {
    private InnerJoinRestrictionFlatMapFunction<Op> innerJoinRestrictionFlatMapFunction;
    protected LocatedRow leftRow;
    public CogroupInnerJoinRestrictionFlatMapFunction() {
        super();
    }

    public CogroupInnerJoinRestrictionFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public Iterator<LocatedRow> call(Tuple2<Iterable<LocatedRow>, Iterable<LocatedRow>> tuple) throws Exception {
        checkInit();
        Iterable<LocatedRow> rightSide = tuple._2; // Memory Issue, HashSet ?
        List<Iterable<LocatedRow>> returnRows = new LinkedList<>();
        for(LocatedRow a_1 : tuple._1){
            returnRows.add(IteratorUtils.toList(innerJoinRestrictionFlatMapFunction.call(new Tuple2<>(a_1,rightSide))));
        }
        return new ConcatenatedIterable<>(returnRows).iterator();
    }
    @Override
    protected void checkInit() {
        if (!initialized)
            innerJoinRestrictionFlatMapFunction = new InnerJoinRestrictionFlatMapFunction<>(operationContext);
        super.checkInit();
    }

}