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

import com.google.common.base.Optional;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import scala.Tuple2;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 4/22/15.
 */
public class OuterJoinFunction<Op extends SpliceOperation> extends SpliceJoinFunction<Op, Tuple2<LocatedRow,Optional<LocatedRow>>, LocatedRow> {
    private static final long serialVersionUID = 3988079974858059941L;

    public OuterJoinFunction() {
    }

    public OuterJoinFunction(OperationContext<Op> operationContext) {
        super(operationContext);

    }

    @Override
    public LocatedRow call(Tuple2<LocatedRow, Optional<LocatedRow>> tuple) throws Exception {
        checkInit();
        LocatedRow lr;
        if (tuple._2.isPresent())
            lr = new LocatedRow(JoinUtils.getMergedRow(tuple._1.getRow(),
                    tuple._2.get().getRow(),op.wasRightOuterJoin,executionFactory.getValueRow(numberOfColumns)));
        else
            lr = new LocatedRow(JoinUtils.getMergedRow(tuple._1.getRow(),
                    op.getEmptyRow(),op.wasRightOuterJoin,executionFactory.getValueRow(numberOfColumns)));
        op.setCurrentLocatedRow(lr);
        if (!op.getRestriction().apply(lr.getRow())) {
            lr = new LocatedRow(JoinUtils.getMergedRow(tuple._1.getRow(),
                    op.getEmptyRow(), op.wasRightOuterJoin, executionFactory.getValueRow(numberOfColumns)));
            op.setCurrentLocatedRow(lr);
            operationContext.recordFilter();
        }
        return lr;
    }
}
