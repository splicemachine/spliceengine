/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.GenericAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.stream.iapi.OperationContext;

import java.io.Serializable;

/**
 * Created by jleach on 4/24/15.
 */
public class MergeNonDistinctAggregatesFunction<Op extends SpliceOperation> extends SpliceFunction2<Op, ExecRow, ExecRow, ExecRow> implements Serializable {
    protected SpliceGenericAggregator[] aggregates;
    protected boolean initialized;
    protected GenericAggregateOperation op;

    public MergeNonDistinctAggregatesFunction() {
    }

    public MergeNonDistinctAggregatesFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public ExecRow call(ExecRow locatedRow1, ExecRow locatedRow2) throws Exception {
        if (!initialized) {
            op = (GenericAggregateOperation) getOperation();
            aggregates = op.aggregates;
            initialized = true;
        }
        operationContext.recordRead();

        if (locatedRow1 == null) return locatedRow2.getClone();
        if (locatedRow2 == null) return locatedRow1;
        ExecRow r1 = locatedRow1;
        ExecRow r2 = locatedRow2;

        for (SpliceGenericAggregator aggregator : aggregates) {
            if (!aggregator.isDistinct()) {
                if (!aggregator.isInitialized(locatedRow1)) {
                    aggregator.initializeAndAccumulateIfNeeded(r1, r1);
                }
                if (!aggregator.isInitialized(locatedRow2)) {
                    aggregator.initializeAndAccumulateIfNeeded(r2, r2);
                }
                aggregator.merge(r2, r1);
            }
        }
        return r1;
    }
}
