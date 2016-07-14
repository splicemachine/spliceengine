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
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.utils.StreamLogUtils;
import com.splicemachine.primitives.Bytes;
import scala.Tuple2;

/**
 * Created by jleach on 5/19/15.
 */
public class InsertPairFunction extends SplicePairFunction<SpliceOperation,LocatedRow,RowLocation,ExecRow> {
    private int counter = 0;
    public InsertPairFunction() {
       super();
    }

    public InsertPairFunction(OperationContext<SpliceOperation> operationContext) {
        super(operationContext);
    }

    @Override
    public Tuple2<RowLocation, ExecRow> call(LocatedRow locatedRow) throws Exception {
        return new Tuple2<>(locatedRow.getRowLocation(),locatedRow.getRow());
    }
    
    @Override
    public RowLocation genKey(LocatedRow locatedRow) {
        counter++;
        RowLocation rowLocation = locatedRow.getRowLocation();
        return rowLocation==null?new HBaseRowLocation(Bytes.toBytes(counter)):(HBaseRowLocation) rowLocation.cloneValue(true);
    }
    
    @Override
    public ExecRow genValue(LocatedRow locatedRow) {
        StreamLogUtils.logOperationRecordWithMessage(locatedRow, operationContext, "indexed for insert");
        return locatedRow.getRow();
    }
}
