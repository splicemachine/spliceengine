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

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.GenericAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.stream.iapi.OperationContext;

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
        if (!initialized) {
            op = (GenericAggregateOperation) getOperation();
            aggregates = op.aggregates;
            initialized = true;
        }

        if (!locatedRows.hasNext()) {
            if (returnDefault) {
                ExecRow valueRow = op.getSourceExecIndexRow().getClone();
                op.finishAggregation(valueRow);
                return Collections.singletonList(new LocatedRow(valueRow));
            } else
                return Collections.EMPTY_LIST;
        }

        ExecRow r1 = locatedRows.next().getRow();
        for (SpliceGenericAggregator aggregator:aggregates) {
            if (!aggregator.isInitialized(r1)) {
//                if (RDDUtils.LOG.isTraceEnabled()) {
//                    RDDUtils.LOG.trace(String.format("Initializing and accumulating %s", r1));
//                }
                aggregator.initializeAndAccumulateIfNeeded(r1, r1);
            }
        }
        while (locatedRows.hasNext()) {
            ExecRow r2 = locatedRows.next().getRow();                                                                                                            
            for (SpliceGenericAggregator aggregator:aggregates) {
                if (!aggregator.isInitialized(r2)) {
//                    if (RDDUtils.LOG.isTraceEnabled()) {
//                        RDDUtils.LOG.trace(String.format("Accumulating %s to %s", r2, r1));
//                    }
                    aggregator.accumulate(r2, r1);
                } else {
//                    if (RDDUtils.LOG.isTraceEnabled()) {
//                        RDDUtils.LOG.trace(String.format("Merging %s to %s", r2, r1));
//                    }
                    aggregator.merge(r2, r1);
                }
            }
        }
        op.finishAggregation(r1); // calls setCurrentRow
        return Collections.singletonList(new LocatedRow(r1));
    }
}
