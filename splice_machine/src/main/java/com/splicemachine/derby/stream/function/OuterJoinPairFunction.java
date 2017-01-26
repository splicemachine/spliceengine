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
