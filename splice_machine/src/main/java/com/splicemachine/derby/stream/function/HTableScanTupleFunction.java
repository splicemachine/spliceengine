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
import com.splicemachine.derby.stream.iapi.OperationContext;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by jyuan on 10/19/15.
 */
public class HTableScanTupleFunction<Op extends SpliceOperation, T> extends SpliceFunction<Op, Tuple2<byte[],T>,T> implements Serializable {

    public HTableScanTupleFunction() {
        super();
    }

    public HTableScanTupleFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public T call(Tuple2<byte[], T> tuple) throws Exception {
        return tuple._2();
    }

}
