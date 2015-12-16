package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.GenericAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.spark.RDDUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;

public class MergeAllAggregatesFlatMapFunction<Op extends com.splicemachine.derby.iapi.sql.execute.SpliceOperation>
    extends SpliceFlatMapFunction<Op, Iterator<LocatedRow>, LocatedRow> {
    
    private static final long serialVersionUID = 1561171735864568225L;
    
    protected boolean initialized;
    protected boolean returnDefault;
    protected GenericAggregateOperation op;
    protected SpliceGenericAggregator[] aggregates;

    public MergeAllAggregatesFlatMapFunction() {
    }

    public MergeAllAggregatesFlatMapFunction(OperationContext<Op> operationContext, boolean returnDefault) {
        super(operationContext);
        this.returnDefault = returnDefault;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(returnDefault);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        returnDefault = in.readBoolean();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<LocatedRow> call(Iterator<LocatedRow> locatedRows) throws Exception {
        if (!locatedRows.hasNext()) {
            return returnDefault ?
                Collections.singletonList(new LocatedRow(getOperation().getExecRowDefinition())) :
                Collections.EMPTY_LIST;
        }
        if (!initialized) {
            op = (GenericAggregateOperation) getOperation();
            aggregates = op.aggregates;
            initialized = true;
        }
        ExecRow r1 = locatedRows.next().getRow();
        for (SpliceGenericAggregator aggregator:aggregates) {
            if (!aggregator.isInitialized(r1)) {
                if (RDDUtils.LOG.isTraceEnabled()) {
                    RDDUtils.LOG.trace(String.format("Initializing and accumulating %s", r1));
                }
                aggregator.initializeAndAccumulateIfNeeded(r1, r1);
            }
        }
        while (locatedRows.hasNext()) {
            ExecRow r2 = locatedRows.next().getRow();                                                                                                            
            for (SpliceGenericAggregator aggregator:aggregates) {
                if (!aggregator.isInitialized(r2)) {
                    if (RDDUtils.LOG.isTraceEnabled()) {
                        RDDUtils.LOG.trace(String.format("Accumulating %s to %s", r2, r1));
                    }
                    aggregator.accumulate(r2, r1);
                } else {
                    if (RDDUtils.LOG.isTraceEnabled()) {
                        RDDUtils.LOG.trace(String.format("Merging %s to %s", r2, r1));
                    }
                    aggregator.merge(r2, r1);
                }
            }
        }
        op.finishAggregation(r1); // calls setCurrentRow
        return Collections.singletonList(new LocatedRow(r1));
    }
}
