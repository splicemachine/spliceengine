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
import com.splicemachine.derby.impl.sql.execute.operations.GenericAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.stream.iapi.OperationContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * Created by jleach on 4/24/15.
 */
public class MergeNonDistinctAggregatesFunction<Op extends SpliceOperation> extends SpliceFunction2<Op, LocatedRow, LocatedRow, LocatedRow> implements Serializable {
    protected SpliceGenericAggregator[] aggregates;
    protected boolean initialized;
    protected GenericAggregateOperation op;

    public MergeNonDistinctAggregatesFunction() {
    }

    public MergeNonDistinctAggregatesFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public LocatedRow call(LocatedRow locatedRow1, LocatedRow locatedRow2) throws Exception {
        if (!initialized) {
            op = (GenericAggregateOperation) getOperation();
            aggregates = op.aggregates;
            initialized = true;
        }
        operationContext.recordRead();

        if (locatedRow1 == null) return locatedRow2.getClone();
        if (locatedRow2 == null) return locatedRow1;
        ExecRow r1 = locatedRow1.getRow();
        ExecRow r2 = locatedRow2.getRow();

        for (SpliceGenericAggregator aggregator : aggregates) {
            if (!aggregator.isDistinct()) {
                if (!aggregator.isInitialized(locatedRow1.getRow())) {
                    aggregator.initializeAndAccumulateIfNeeded(r1, r1);
                }
                if (!aggregator.isInitialized(locatedRow2.getRow())) {
                    aggregator.initializeAndAccumulateIfNeeded(r2, r2);
                }
                aggregator.merge(r2, r1);
            }
        }
        return new LocatedRow(locatedRow1.getRowLocation(), r1);
    }
}