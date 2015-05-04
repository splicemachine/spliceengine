package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.IndexValueRow;
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

public class MergeAllAggregatesFunction<Op extends com.splicemachine.derby.iapi.sql.execute.SpliceOperation> extends SpliceFunction2<Op,LocatedRow,LocatedRow,LocatedRow> implements Serializable {
    protected SpliceGenericAggregator[] aggregates;
    public MergeAllAggregatesFunction() {
    }

    public MergeAllAggregatesFunction (OperationContext<Op> operationContext,SpliceGenericAggregator[] aggregates) {
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
    public LocatedRow call(LocatedRow locatedRow1, LocatedRow locatedRow2) throws Exception {
        if (locatedRow1 == null) return locatedRow2;
        if (locatedRow2 == null) return locatedRow1;
        ExecRow r1 = locatedRow1.getRow();
        ExecRow r2 = locatedRow2.getRow();

        if (!(r1 instanceof ExecIndexRow)) {
            r1 = new IndexValueRow(r1.getClone());
        }

        for(SpliceGenericAggregator aggregator:aggregates) {
            if (!aggregator.isInitialized(r1)) {
                aggregator.initializeAndAccumulateIfNeeded(r1, r1);
            }
            if (!aggregator.isInitialized(r2)) {
                aggregator.initializeAndAccumulateIfNeeded(r2, r2);
            }
            aggregator.merge(r2, r1);
        }
        return new LocatedRow(locatedRow1.getRowLocation(),r1);

    }

}
