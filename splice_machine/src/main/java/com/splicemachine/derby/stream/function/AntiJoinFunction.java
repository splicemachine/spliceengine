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
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 4/22/15.
 */
public class AntiJoinFunction<Op extends SpliceOperation> extends SpliceJoinFunction<Op, LocatedRow, LocatedRow> {
    private static final long serialVersionUID = 3988079974858059941L;
    public AntiJoinFunction() {
    }

    public AntiJoinFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public LocatedRow call(LocatedRow inputRow) throws Exception {
        checkInit();
        LocatedRow lr = new LocatedRow(inputRow.getRowLocation(),JoinUtils.getMergedRow(inputRow.getRow(),
                op.getEmptyRow(), op.wasRightOuterJoin,executionFactory.getValueRow(numberOfColumns)));
        op.setCurrentLocatedRow(lr);
        return lr;
    }

}
