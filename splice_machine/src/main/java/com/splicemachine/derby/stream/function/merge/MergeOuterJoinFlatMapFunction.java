package com.splicemachine.derby.stream.function.merge;

import com.splicemachine.derby.impl.sql.execute.operations.*;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iterator.merge.AbstractMergeJoinIterator;
import com.splicemachine.derby.stream.iterator.merge.MergeOuterJoinIterator;
import org.sparkproject.guava.collect.PeekingIterator;

/**
 * Created by jleach on 6/9/15.
 */
public class MergeOuterJoinFlatMapFunction extends AbstractMergeJoinFlatMapFunction {
    public MergeOuterJoinFlatMapFunction() {
        super();
    }

    public MergeOuterJoinFlatMapFunction(OperationContext<MergeJoinOperation> operationContext) {
        super(operationContext);
    }

    @Override
    protected AbstractMergeJoinIterator createMergeJoinIterator(PeekingIterator<LocatedRow> leftPeekingIterator, PeekingIterator<LocatedRow> rightPeekingIterator, int[] leftHashKeys, int[] rightHashKeys, MergeJoinOperation mergeJoinOperation, OperationContext<MergeJoinOperation> operationContext) {
        return new MergeOuterJoinIterator(leftPeekingIterator,
                rightPeekingIterator,
                mergeJoinOperation.leftHashKeys, mergeJoinOperation.rightHashKeys,
                mergeJoinOperation, operationContext);
    }
}
