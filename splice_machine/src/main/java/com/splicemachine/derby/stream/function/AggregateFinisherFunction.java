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
import com.splicemachine.derby.impl.sql.execute.operations.GenericAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.GroupedAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.stream.iapi.OperationContext;

/**
 * Created by jleach on 4/24/15.
 */
public class AggregateFinisherFunction extends SpliceFunction<GroupedAggregateOperation, ExecRow, ExecRow> {
        protected SpliceGenericAggregator[] aggregates;
        protected boolean initialized;
        protected GenericAggregateOperation op;
        public AggregateFinisherFunction() {
            super();
        }

        public AggregateFinisherFunction(OperationContext<GroupedAggregateOperation> operationContext) {
            super(operationContext);
        }
        @Override
        public ExecRow call(ExecRow locatedRow) throws Exception {
            if (!initialized) {
                op =getOperation();
                aggregates = op.aggregates;
                initialized = true;
            }
            for(SpliceGenericAggregator aggregator:aggregates){
                if (!aggregator.isInitialized(locatedRow)) {
                    aggregator.initializeAndAccumulateIfNeeded(locatedRow, locatedRow);
                }
                aggregator.finish(locatedRow);
            }
            op.setCurrentRow(locatedRow);
            operationContext.recordProduced();
            return locatedRow;
        }
}
