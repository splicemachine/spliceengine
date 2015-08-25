package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.IndexValueRow;
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
public class MergeNonDistinctAggregatesFunction<Op extends SpliceOperation> extends SpliceFunction2<Op,LocatedRow,LocatedRow,LocatedRow> implements Serializable {
    protected SpliceGenericAggregator[] aggregates;
    protected boolean initialized;
    protected GenericAggregateOperation op;
    public MergeNonDistinctAggregatesFunction() {
    }

    public MergeNonDistinctAggregatesFunction (OperationContext<Op> operationContext) {
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
    public LocatedRow call(LocatedRow locatedRow1, LocatedRow locatedRow2) throws Exception {
        if (!initialized) {
            op = (GenericAggregateOperation) getOperation();
            aggregates = op.aggregates;
            initialized = true;
        }
            if (locatedRow1 == null) return locatedRow2;
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
            return new LocatedRow(locatedRow1.getRowLocation(),r1);
    }
}