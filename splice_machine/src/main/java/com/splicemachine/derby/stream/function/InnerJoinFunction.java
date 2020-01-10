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

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.stream.iapi.OperationContext;
import scala.Tuple2;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 */
@NotThreadSafe
public class InnerJoinFunction<Op extends SpliceOperation> extends SpliceJoinFunction<Op, Tuple2<ExecRow,Tuple2<ExecRow,ExecRow>>, ExecRow> {
    private static final long serialVersionUID = 3988079974858059941L;
    public InnerJoinFunction() {
    }

    public InnerJoinFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
    }

    @Override
    public ExecRow call(Tuple2<ExecRow, Tuple2<ExecRow, ExecRow>> tuple) throws Exception {
        checkInit();
        ExecRow execRow = JoinUtils.getMergedRow(tuple._2()._1(),tuple._2()._2(),
                op.wasRightOuterJoin,executionFactory.getValueRow(numberOfColumns));
        op.setCurrentRow(execRow);
        return execRow;
    }
}
