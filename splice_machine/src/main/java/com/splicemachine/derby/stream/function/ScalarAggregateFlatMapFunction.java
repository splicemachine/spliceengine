/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.ScalarAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.commons.collections.iterators.SingletonIterator;

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
    public Iterator<LocatedRow> call(Iterator<LocatedRow> locatedRows) throws Exception {
        if (!locatedRows.hasNext()) {
            return returnDefault ?
                new SingletonIterator(new LocatedRow(getOperation().getExecRowDefinition())) :
                Collections.EMPTY_LIST.iterator();
        }
        if (!initialized) {
            op = getOperation();
            initialized = true;
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
        return new SingletonIterator(new LocatedRow(r1));
    }
}
