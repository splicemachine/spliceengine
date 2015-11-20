package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.IterableJoinFunction;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iterator.NestedLoopInnerIterator;
import com.splicemachine.derby.stream.utils.StreamUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;

/**
 * Created by jleach on 4/24/15.
 */
public class NLJInnerJoinFunction<Op extends SpliceOperation> extends SpliceJoinFlatMapFunction<Op, LocatedRow, LocatedRow> implements IterableJoinFunction {

    public Iterator<LocatedRow> rightSideNLJIterator;
    public LocatedRow leftRow;

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
    public Iterable<LocatedRow> call(LocatedRow from) throws Exception {
        checkInit();
        leftRow = from;
        DataSet dataSet = null;
        try {
            op.getRightOperation().openCore(StreamUtils.controlDataSetProcessor);
            rightSideNLJIterator = op.getRightOperation().getLocatedRowIterator();
            return new NestedLoopInnerIterator<>(this);
        } finally {
            if (op.getRightOperation()!= null)
                op.getRightOperation().close();
        }

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
    
    public String getPrettyFunctionName() {
        return "Nested Loop Inner Join";
    }

}