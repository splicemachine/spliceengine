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

import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 * This class filters out nulls based on the hash keys provided.
 *
 */
public class InnerJoinNullFilterFunction extends SplicePredicateFunction<JoinOperation,LocatedRow> {
    private boolean initialized = false;
    private int[] hashKeys;
    public InnerJoinNullFilterFunction() {
        super();
    }

    public InnerJoinNullFilterFunction(OperationContext<JoinOperation> operationContext, int[] hashKeys) {
        super(operationContext);
        assert hashKeys!=null && hashKeys.length >0 : "Bad Hash Keys Passed into Null Filter Function";
        this.hashKeys = hashKeys;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        ArrayUtil.writeIntArray(out,hashKeys);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        hashKeys = ArrayUtil.readIntArray(in);
    }

    @Override
    public boolean apply(@Nullable LocatedRow locatedRow) {
        try {
            ExecRow row = locatedRow.getRow();
            for (int i = 0; i< hashKeys.length; i++) {
                if (row.getColumn(hashKeys[i]+1).isNull()) {
                    operationContext.recordFilter();
                    return false;
                }
            }
            operationContext.getOperation().setCurrentLocatedRow(locatedRow);
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
