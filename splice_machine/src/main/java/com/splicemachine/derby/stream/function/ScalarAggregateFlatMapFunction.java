/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.ScalarAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.stream.iapi.OperationContext;

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
//        if (RDDUtils.LOG.isTraceEnabled()) {
//            RDDUtils.LOG.trace(String.format("Accumulating %s to %s", next, agg));
//        }
        for (SpliceGenericAggregator aggregate : op.aggregates)
            aggregate.accumulate(next, agg);
    }

    private void merge(ExecRow next, ExecRow agg) throws StandardException {
        ScalarAggregateOperation op = (ScalarAggregateOperation) getOperation();
//        if (RDDUtils.LOG.isTraceEnabled()) {
//            RDDUtils.LOG.trace(String.format("Merging %s to %s", next, agg));
//        }
        for (SpliceGenericAggregator aggregate : op.aggregates)
            aggregate.merge(next, agg);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<LocatedRow> call(Iterator<LocatedRow> locatedRows) throws Exception {
        if (!initialized) {
            op = getOperation();
            initialized = true;
        }

        if (!locatedRows.hasNext()) {
            if (returnDefault) {
                ExecRow valueRow = getOperation().getSourceExecIndexRow().getClone();
                op.finishAggregation(valueRow);
                return Collections.singletonList(new LocatedRow(valueRow));
            } else
                return Collections.EMPTY_LIST;
        }

        ExecRow r1 = locatedRows.next().getRow();
        if (!op.isInitialized(r1)) {
//            if (RDDUtils.LOG.isTraceEnabled()) {
//                RDDUtils.LOG.trace(String.format("Initializing and accumulating %s", r1));
//            }
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
