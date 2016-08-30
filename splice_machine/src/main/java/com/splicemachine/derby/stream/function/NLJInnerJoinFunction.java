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

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.IterableJoinFunction;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iterator.NestedLoopInnerIterator;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;

/**
 *
 */
public class NLJInnerJoinFunction<Op extends SpliceOperation> extends SpliceJoinFlatMapFunction<Op, LocatedRow, LocatedRow> implements IterableJoinFunction {

    public Iterator<LocatedRow> rightSideNLJIterator;
    public LocatedRow leftRow;
    private boolean opened = false;

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
    public Iterator<LocatedRow> call(LocatedRow from) throws Exception {
        checkInit();
        leftRow = from;
        op.getLeftOperation().setCurrentLocatedRow(from);
        SpliceOperation rightOperation=op.getRightOperation();
        if(opened)
            rightOperation.close();
        else
            opened=true;

        rightOperation.openCore(EngineDriver.driver().processorFactory().localProcessor(op.getActivation(),op));
        rightSideNLJIterator = rightOperation.getLocatedRowIterator();
        return new NestedLoopInnerIterator<>(this);
    }

    @Override
    public boolean hasNext() {
        return rightSideNLJIterator.hasNext();
    }

    @Override
    public ExecRow getRightRow() {
        return rightSideNLJIterator.next().getRow();
    }

    @Override
    public ExecRow getLeftRow() {
        return leftRow.getRow();
    }

    @Override
    public RowLocation getLeftRowLocation() {
        return leftRow.getRowLocation();
    }

    @Override
    public boolean wasRightOuterJoin() {
        return op.wasRightOuterJoin;
    }

    @Override
    public ExecutionFactory getExecutionFactory() {
        return executionFactory;
    }

    @Override
    public int getNumberOfColumns() {
        return numberOfColumns;
    }

    @Override
    public void setCurrentLocatedRow(LocatedRow locatedRow) {
        op.setCurrentLocatedRow(locatedRow);
    }

    @Override
    public int getResultSetNumber() {
        return op.resultSetNumber();
    }

    @Override
    public OperationContext getOperationContext() {
        return operationContext;
    }

    @Override
    public LocatedRow getLeftLocatedRow() {
        return leftRow;
    }
}