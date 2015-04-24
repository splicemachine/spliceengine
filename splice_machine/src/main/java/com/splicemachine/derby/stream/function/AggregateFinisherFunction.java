package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.stream.OperationContext;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 4/24/15.
 */
public class AggregateFinisherFunction extends SpliceFunction<SpliceOperation, LocatedRow, LocatedRow> {
        protected SpliceGenericAggregator[] aggregates;
        public AggregateFinisherFunction() {
            super();
        }

        public AggregateFinisherFunction(OperationContext<SpliceOperation> operationContext, SpliceGenericAggregator[] aggregates) {
            super(operationContext);
            this.aggregates = aggregates;
        }
        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            out.writeInt(aggregates.length);
            for (int i = 0; i<aggregates.length;i++) {
                out.writeObject(aggregates[i]);
            }
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);
            aggregates = new SpliceGenericAggregator[in.readInt()];
            for (int i = 0; i<aggregates.length;i++) {
                aggregates[i] = (SpliceGenericAggregator) in.readObject();
            }
        }
        @Override
        public LocatedRow call(LocatedRow locatedRow) throws Exception {
            for(SpliceGenericAggregator aggregator:aggregates){
                if (!aggregator.isInitialized(locatedRow.getRow())) {
                    aggregator.initializeAndAccumulateIfNeeded(locatedRow.getRow(), locatedRow.getRow());
                }
                aggregator.finish(locatedRow.getRow());
            }
            return locatedRow;
        }
}
