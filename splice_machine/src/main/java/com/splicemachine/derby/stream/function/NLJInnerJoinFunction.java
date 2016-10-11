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
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iterator.NestedLoopJoinIterator;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;

/**
 *
 */
public class NLJInnerJoinFunction<Op extends SpliceOperation> extends NLJoinFunction<Op, Iterator<LocatedRow>, LocatedRow> {

    public NLJInnerJoinFunction() {}

    public NLJInnerJoinFunction(OperationContext<Op> operationContext) {
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
    public Iterator<LocatedRow> call(Iterator<LocatedRow> from) throws Exception {
        if (!initialized) {
            init(from);
            initialized = true;
        }

        return new NestedLoopJoinIterator<>(this);
    }

    @Override
    protected void init(Iterator<LocatedRow> from) throws StandardException {
        joinType = JoinType.INNER;
        super.init(from);
    }
}