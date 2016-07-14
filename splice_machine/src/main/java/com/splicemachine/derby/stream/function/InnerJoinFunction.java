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
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
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
public class InnerJoinFunction<Op extends SpliceOperation> extends SpliceJoinFunction<Op, Tuple2<ExecRow,Tuple2<LocatedRow,LocatedRow>>, LocatedRow> {
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
    public LocatedRow call(Tuple2<ExecRow, Tuple2<LocatedRow, LocatedRow>> tuple) throws Exception {
        checkInit();
        ExecRow execRow = JoinUtils.getMergedRow(tuple._2()._1().getRow(),tuple._2()._2().getRow(),
                op.wasRightOuterJoin,executionFactory.getValueRow(numberOfColumns));
        LocatedRow lr = new LocatedRow(tuple._2._1.getRowLocation(),execRow);
        op.setCurrentLocatedRow(lr);
        return lr;
    }
}
