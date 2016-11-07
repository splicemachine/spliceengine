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
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.ProjectRestrictOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.commons.collections.iterators.SingletonIterator;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by jleach on 5/1/15.
 */
public class ProjectRestrictFlatMapFunction<Op extends SpliceOperation> extends SpliceFlatMapFunction<Op,LocatedRow,LocatedRow> {
    protected boolean initialized;
    protected ProjectRestrictOperation op;
    protected ExecutionFactory executionFactory;

    public ProjectRestrictFlatMapFunction() {
        super();
    }

    public ProjectRestrictFlatMapFunction(OperationContext<Op> operationContext) {
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
    public Iterator<LocatedRow> call(LocatedRow from) throws Exception {
        if (!initialized) {
            initialized = true;
            op = (ProjectRestrictOperation) getOperation();
            executionFactory = op.getExecutionFactory();
        }
        op.setCurrentRow(from.getRow());
        op.source.setCurrentRow(from.getRow());
        if (!op.getRestriction().apply(from.getRow())) {
            operationContext.recordFilter();
            return Collections.<LocatedRow>emptyList().iterator();
        }
        ExecRow preCopy = op.doProjection(from.getRow());
        LocatedRow locatedRow = new LocatedRow(from.getRowLocation(), preCopy);
        op.setCurrentLocatedRow(locatedRow);
        return new SingletonIterator(locatedRow);
    }
}
