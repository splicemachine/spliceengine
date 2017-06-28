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

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.utils.ConcatenatedIterable;
import org.apache.commons.collections.IteratorUtils;
import scala.Tuple2;
import java.util.*;

/**
 *
 *
 */
public class CogroupAntiJoinRestrictionFlatMapFunction<Op extends SpliceOperation> extends SpliceJoinFlatMapFunction<Op, Tuple2<Iterable<ExecRow>,Iterable<ExecRow>>,ExecRow> {
    private AntiJoinRestrictionFlatMapFunction<Op> antiJoinRestrictionFlatMapFunction;
    protected ExecRow leftRow;
    public CogroupAntiJoinRestrictionFlatMapFunction() {
        super();
    }

    public CogroupAntiJoinRestrictionFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public Iterator<ExecRow> call(Tuple2<Iterable<ExecRow>, Iterable<ExecRow>> tuple) throws Exception {
        checkInit();
        Iterable<ExecRow> rightSide = tuple._2;
        List<Iterable<ExecRow>> returnRows = new LinkedList<>();
        for(ExecRow a_1 : tuple._1){
            returnRows.add(IteratorUtils.toList(antiJoinRestrictionFlatMapFunction.call(new Tuple2<>(a_1,rightSide))));
        }
        return new ConcatenatedIterable<>(returnRows).iterator();
    }

    @Override
    protected void checkInit() {
        if (!initialized)
            antiJoinRestrictionFlatMapFunction = new AntiJoinRestrictionFlatMapFunction<>(operationContext);
        super.checkInit();
    }
}
