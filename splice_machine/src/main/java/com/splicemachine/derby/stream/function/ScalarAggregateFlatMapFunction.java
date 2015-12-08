package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.ScalarAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.spark.RDDUtils;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by jleach on 5/1/15.
 */
public class ScalarAggregateFlatMapFunction extends SpliceFlatMapFunction<ScalarAggregateOperation, Iterator<LocatedRow>, LocatedRow> {
    private static final long serialVersionUID = -4150499166764796082L;
    protected boolean initialized;
    protected ScalarAggregateOperation op;
    protected boolean returnDefault;
    public ScalarAggregateFlatMapFunction() {
    }

    public ScalarAggregateFlatMapFunction(OperationContext<ScalarAggregateOperation> operationContext, boolean returnDefault) {
        super(operationContext);
        this.returnDefault = returnDefault;
    }

    @Override
    public Iterable<LocatedRow> call(Iterator<LocatedRow> locatedRows) throws Exception {
        if (!locatedRows.hasNext())
            return Collections.EMPTY_LIST;
        if (!initialized) {
            op = getOperation();
            initialized = true;
        }
        ExecRow r1 = locatedRows.next().getRow();
        if (!op.isInitialized(r1)) {
            op.initializeVectorAggregation(r1);
        }
        while (locatedRows.hasNext()) {
            ExecRow r2 = locatedRows.next().getRow().getClone(); // Why do we need to clone?
            if (!op.isInitialized(r2)) {
                accumulate(r2, r1); // Accumulate
            } else {
                merge(r2, r1); // Merge computations
            }
        }
        return Collections.singletonList(new LocatedRow(r1));
    }

    private void accumulate(ExecRow next, ExecRow agg) throws StandardException {
        ScalarAggregateOperation op = (ScalarAggregateOperation) getOperation();
        if (RDDUtils.LOG.isDebugEnabled()) {
            RDDUtils.LOG.debug(String.format("Accumulating %s to %s", next, agg));
        }
        for (SpliceGenericAggregator aggregate : op.aggregates)
            aggregate.accumulate(next, agg);
    }

    private void merge(ExecRow next, ExecRow agg) throws StandardException {
        ScalarAggregateOperation op = (ScalarAggregateOperation) getOperation();
        if (RDDUtils.LOG.isDebugEnabled()) {
            RDDUtils.LOG.debug(String.format("Merging %s to %s", next, agg));
        }
        for (SpliceGenericAggregator aggregate : op.aggregates)
            aggregate.merge(next, agg);
    }

}

