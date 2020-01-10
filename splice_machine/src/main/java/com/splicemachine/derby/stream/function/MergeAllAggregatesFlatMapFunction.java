/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.GenericAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.commons.collections.iterators.SingletonIterator;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;

public class MergeAllAggregatesFlatMapFunction<Op extends com.splicemachine.derby.iapi.sql.execute.SpliceOperation>
    extends SpliceFlatMapFunction<Op, Iterator<ExecRow>, ExecRow> {

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
    public Iterator<ExecRow> call(Iterator<ExecRow> locatedRows) throws Exception {
        if (!initialized) {
            op = (GenericAggregateOperation) getOperation();
            aggregates = op.aggregates;
            initialized = true;
        }

        if (!locatedRows.hasNext()) {
            if (returnDefault) {
                ExecRow valueRow = op.getSourceExecIndexRow().getClone();
                op.finishAggregation(valueRow);
                return new SingletonIterator(valueRow);
            } else
                return Collections.EMPTY_LIST.iterator();
        }

        ExecRow r1 = locatedRows.next();
        for (SpliceGenericAggregator aggregator:aggregates) {
            if (!aggregator.isInitialized(r1)) {
//                if (RDDUtils.LOG.isTraceEnabled()) {
//                    RDDUtils.LOG.trace(String.format("Initializing and accumulating %s", r1));
//                }
                aggregator.initializeAndAccumulateIfNeeded(r1, r1);
            }
        }
        while (locatedRows.hasNext()) {
            ExecRow r2 = locatedRows.next();
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
        return new SingletonIterator(r1);
    }
}
