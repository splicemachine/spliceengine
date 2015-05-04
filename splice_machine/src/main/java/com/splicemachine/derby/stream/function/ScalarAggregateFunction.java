package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.IndexValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.ScalarAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.spark.RDDUtils;

/**
 * Created by jleach on 5/1/15.
 */
public class ScalarAggregateFunction extends SpliceFunction2<SpliceOperation, LocatedRow, LocatedRow, LocatedRow> {
    private static final long serialVersionUID = -4150499166764796082L;
    protected boolean initialized;
    protected ScalarAggregateOperation op;
    public ScalarAggregateFunction() {
    }

    public ScalarAggregateFunction(OperationContext<SpliceOperation> operationContext) {
        super(operationContext);
    }

    @Override
    public LocatedRow call(LocatedRow t1, LocatedRow t2) throws Exception {
        if (!initialized) {
            op = (ScalarAggregateOperation) getOperation();
            initialized = true;
        }
        operationContext.recordRead();
        System.out.println("t1 -> " + t1);
        System.out.println("t2 -> " + t2);
        if (t2 == null) return t1;
        if (t1 == null) return t2;
        if (RDDUtils.LOG.isDebugEnabled())
            RDDUtils.LOG.debug(String.format("Reducing %s and %s", t1, t2));

        ExecRow r1 = t1.getRow();
        ExecRow r2 = t2.getRow();
        if (!(r1 instanceof ExecIndexRow)) {
            r1 = new IndexValueRow(r1.getClone());
        }
        if (!op.isInitialized(r1)) {
            op.initializeVectorAggregation(r1);
        }
        if (!op.isInitialized(r2)) {
            op.initializeVectorAggregation(r2);
        }
        aggregate(t2.getRow(), (ExecIndexRow) r1);
        System.out.println("r1 -> " + r1);
        return new LocatedRow(r1);
    }

    private void aggregate(ExecRow next, ExecIndexRow agg) throws StandardException {
        ScalarAggregateOperation op = (ScalarAggregateOperation) getOperation();
        if (RDDUtils.LOG.isDebugEnabled()) {
            RDDUtils.LOG.debug(String.format("Merging %s to %s", next, agg));
        }
        for (SpliceGenericAggregator aggregate : op.aggregates)
            aggregate.merge(next, agg);
    }

}

