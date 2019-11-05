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

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.stream.iapi.OperationContext;

/**
 * Created by jleach on 4/22/15.
 */
public class AntiJoinFunction<Op extends SpliceOperation> extends SpliceJoinFunction<Op, ExecRow, ExecRow> {
    private static final long serialVersionUID = 3988079974858059941L;
    public AntiJoinFunction() {
    }

    public AntiJoinFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public ExecRow call(ExecRow inputRow) throws Exception {
        checkInit();
        ExecRow lr = JoinUtils.getMergedRow(inputRow,
                op.getRightEmptyRow(), op.wasRightOuterJoin,executionFactory.getValueRow(numberOfColumns));
        op.setCurrentRow(lr);
        return lr;
    }

}
