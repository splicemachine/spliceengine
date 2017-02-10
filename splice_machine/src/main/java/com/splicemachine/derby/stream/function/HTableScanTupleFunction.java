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
