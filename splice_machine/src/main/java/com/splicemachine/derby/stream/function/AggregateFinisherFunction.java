package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.GenericAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.GroupedAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.stream.iapi.OperationContext;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 4/24/15.
 */
public class AggregateFinisherFunction extends SpliceFunction<GroupedAggregateOperation, LocatedRow, LocatedRow> {
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
        public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);
        }
        @Override
        public LocatedRow call(LocatedRow locatedRow) throws Exception {
            if (!initialized) {
                op = (GenericAggregateOperation) getOperation();
                aggregates = op.aggregates;
                initialized = true;
            }
            for(SpliceGenericAggregator aggregator:aggregates){
                if (!aggregator.isInitialized(locatedRow.getRow())) {
                    aggregator.initializeAndAccumulateIfNeeded(locatedRow.getRow(), locatedRow.getRow());
                }
                aggregator.finish(locatedRow.getRow());
            }
            op.setCurrentLocatedRow(locatedRow);
            return locatedRow;
        }
}
