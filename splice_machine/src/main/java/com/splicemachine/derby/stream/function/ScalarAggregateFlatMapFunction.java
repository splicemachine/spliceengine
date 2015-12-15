package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.ScalarAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.spark.RDDUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;

public class ScalarAggregateFlatMapFunction
    extends SpliceFlatMapFunction<ScalarAggregateOperation, Iterator<LocatedRow>, LocatedRow> {
    
    private static final long serialVersionUID = 844136943916989111L;
    
    protected boolean initialized;
    protected boolean returnDefault;
    protected ScalarAggregateOperation op;
    
    public ScalarAggregateFlatMapFunction() {
    }

    public ScalarAggregateFlatMapFunction(OperationContext<ScalarAggregateOperation> operationContext, boolean returnDefault) {
        super(operationContext);
        this.returnDefault = returnDefault;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(returnDefault);
    }

    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException {
        super.readExternal(in);
        returnDefault = in.readBoolean();
    }

    private void accumulate(ExecRow next, ExecRow agg) throws StandardException {
        ScalarAggregateOperation op = (ScalarAggregateOperation) getOperation();
        if (RDDUtils.LOG.isTraceEnabled()) {
            RDDUtils.LOG.trace(String.format("Accumulating %s to %s", next, agg));
        }
        for (SpliceGenericAggregator aggregate : op.aggregates)
            aggregate.accumulate(next, agg);
    }

    private void merge(ExecRow next, ExecRow agg) throws StandardException {
        ScalarAggregateOperation op = (ScalarAggregateOperation) getOperation();
        if (RDDUtils.LOG.isTraceEnabled()) {
            RDDUtils.LOG.trace(String.format("Merging %s to %s", next, agg));
        }
        for (SpliceGenericAggregator aggregate : op.aggregates)
            aggregate.merge(next, agg);
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
            op = getOperation();
            initialized = true;
        }
        ExecRow r1 = locatedRows.next().getRow();
        if (!op.isInitialized(r1)) {
            if (RDDUtils.LOG.isTraceEnabled()) {
                RDDUtils.LOG.trace(String.format("Initializing and accumulating %s", r1));
            }
            op.initializeVectorAggregation(r1);
        }
        while (locatedRows.hasNext()) {
            ExecRow r2 = locatedRows.next().getRow();                                                                                                            
            if (!op.isInitialized(r2)) {
                accumulate(r2, r1);                                                                                                                                                      
            } else {
                merge(r2, r1);                                                                                                                                                  
            }
        }
        op.finishAggregation(r1); // calls setCurrentRow
        return Collections.singletonList(new LocatedRow(r1));
    }
}
