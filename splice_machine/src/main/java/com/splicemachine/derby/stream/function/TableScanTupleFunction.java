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
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by jleach on 4/20/15.
 */
public class TableScanTupleFunction<Op extends SpliceOperation> extends SpliceFunction<Op, Tuple2<RowLocation,ExecRow>,LocatedRow> implements Serializable {
    public TableScanTupleFunction() {
        super();
    }

    public TableScanTupleFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public LocatedRow call(Tuple2<RowLocation, ExecRow> tuple) throws Exception {
        this.operationContext.recordRead();
        LocatedRow locatedRow = new LocatedRow(tuple._1(), tuple._2());
        if (operationContext.getOperation() != null) {
            operationContext.getOperation().setCurrentLocatedRow(locatedRow);
        }
        return locatedRow;
    }
}