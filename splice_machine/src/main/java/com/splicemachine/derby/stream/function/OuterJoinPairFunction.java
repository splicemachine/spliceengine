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

import com.google.common.base.Optional;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import scala.Tuple2;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 4/22/15.
 */
public class OuterJoinPairFunction<Op extends SpliceOperation> extends SpliceFunction<Op, Tuple2<ExecRow,Tuple2<LocatedRow,Optional<LocatedRow>>>, LocatedRow> {
    protected OuterJoinFunction<Op> outerJoinFunction = null;
    protected boolean initialized = false;
    private static final long serialVersionUID = 3988079974858059941L;

    public OuterJoinPairFunction() {
    }

    public OuterJoinPairFunction(OperationContext<Op> operationContext) {
        super(operationContext);

    }

    @Override
    public LocatedRow call(Tuple2<ExecRow, Tuple2<LocatedRow, Optional<LocatedRow>>> tuple) throws Exception {
        if (!initialized) {
            outerJoinFunction = new OuterJoinFunction<>(operationContext);
            initialized =  true;
        }
        return outerJoinFunction.call(tuple._2);
    }
}
