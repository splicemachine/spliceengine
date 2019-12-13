/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import org.spark_project.guava.base.Optional;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.stream.iapi.OperationContext;
import scala.Tuple2;

/**
 * Created by jleach on 4/22/15.
 */
public class OuterJoinFunction<Op extends SpliceOperation> extends SpliceJoinFunction<Op, Tuple2<ExecRow,Optional<ExecRow>>, ExecRow> {
    private static final long serialVersionUID = 3988079974858059941L;

    public OuterJoinFunction() {
    }

    public OuterJoinFunction(OperationContext<Op> operationContext) {
        super(operationContext);

    }

    @Override
    public ExecRow call(Tuple2<ExecRow, Optional<ExecRow>> tuple) throws Exception {
        checkInit();
        ExecRow lr;
        if (tuple._2.isPresent())
            lr = JoinUtils.getMergedRow(tuple._1,
                    tuple._2.get(),op.wasRightOuterJoin,executionFactory.getValueRow(numberOfColumns));
        else
            lr = JoinUtils.getMergedRow(tuple._1,
                    op.getRightEmptyRow(),op.wasRightOuterJoin,executionFactory.getValueRow(numberOfColumns));
        op.setCurrentRow(lr);
        if (!op.getRestriction().apply(lr)) {
            lr = JoinUtils.getMergedRow(tuple._1,
                    op.getRightEmptyRow(), op.wasRightOuterJoin, executionFactory.getValueRow(numberOfColumns));
            op.setCurrentRow(lr);
            operationContext.recordFilter();
        }
        return lr;
    }
}
