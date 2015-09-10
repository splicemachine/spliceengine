package com.splicemachine.derby.stream.function.merge;

import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.MergeJoinOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iterator.merge.AbstractMergeJoinIterator;
import com.splicemachine.derby.stream.iterator.merge.MergeAntiJoinIterator;
import org.sparkproject.guava.common.collect.PeekingIterator;

/**
 * Created by jleach on 6/9/15.
 */
public class MergeAntiJoinFlatMapFunction extends AbstractMergeJoinFlatMapFunction {
    public MergeAntiJoinFlatMapFunction() {
        super();
    }

    public MergeAntiJoinFlatMapFunction(OperationContext<MergeJoinOperation> operationContext) {
        super(operationContext);
    }

    @Override
    protected AbstractMergeJoinIterator createMergeJoinIterator(PeekingIterator<LocatedRow> leftPeekingIterator, PeekingIterator<LocatedRow> rightPeekingIterator, int[] leftHashKeys, int[] rightHashKeys, MergeJoinOperation mergeJoinOperation) {
        return new MergeAntiJoinIterator(leftPeekingIterator,
                rightPeekingIterator,
                mergeJoinOperation.leftHashKeys, mergeJoinOperation.rightHashKeys,
                mergeJoinOperation);
    }
}
