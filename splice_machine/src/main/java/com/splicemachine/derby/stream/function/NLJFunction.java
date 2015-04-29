package com.splicemachine.derby.stream.function;

import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.NestedLoopJoinOperation;
import com.splicemachine.derby.stream.DataSet;
import com.splicemachine.derby.stream.OperationContext;
import com.splicemachine.derby.stream.StreamUtils;
import com.splicemachine.derby.stream.iterator.NestedLoopIterator;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by jleach on 4/24/15.
 */
public class NLJFunction<Op extends SpliceOperation> extends SpliceFlatMapFunction<Op, LocatedRow, LocatedRow> {
    protected SpliceOperation rightOperation;
    protected boolean notExistsRightSide;


    public NLJFunction() {}

    public NLJFunction(OperationContext<Op> operationContext, SpliceOperation rightOperation) {
        super(operationContext);
        this.rightOperation = rightOperation;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(rightOperation);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        rightOperation = (SpliceOperation) in.readObject();
    }

    @Override
    public Iterable<LocatedRow> call(LocatedRow from) throws Exception {
        System.out.println("fromRow " + from);
        NestedLoopJoinOperation operation = (NestedLoopJoinOperation) getOperation();
        operation.setCurrentRow(from.getRow());
        DataSet dataSet = rightOperation.getDataSet(StreamUtils.controlDataSetProcessor);
        Iterator<LocatedRow> rightSideNLJ = dataSet.toLocalIterator();

        // Nothing Returned (Outside Iterator)
        if (!rightSideNLJ.hasNext()) {
            System.out.println("nothing on right side");
            if (notExistsRightSide || operation.isOuterJoin) { // Outer Join or Not Exists Evaluation
                ExecRow mergedRow = getNewMergedRow(operation.getRightNumCols(), operation.getLeftNumCols());
                mergedRow = JoinUtils.getMergedRow(from.getRow(), operation.getEmptyRow(), operation.wasRightOuterJoin, operation.getRightNumCols(), operation.getLeftNumCols(), operation.mergedRow);
                if (!operation.getRestriction().apply(mergedRow)) {
                    System.out.println("restricted");
                    return Collections.<LocatedRow>emptyList();
                }
                else {
                    getOperation().setCurrentRow(mergedRow);
                    return Lists.newArrayList(new LocatedRow(mergedRow)); // Not Exists Passed
                }
            } else { // Inner Join: Miss
                return Collections.<LocatedRow>emptyList();
            }
        }
        return new NestedLoopIterator(from,operation,rightSideNLJ);
    }

    private ExecRow getNewMergedRow(int leftNumCols, int rightNumCols) {
        return getActivation().getExecutionFactory().getValueRow(leftNumCols + rightNumCols);
    }

}
