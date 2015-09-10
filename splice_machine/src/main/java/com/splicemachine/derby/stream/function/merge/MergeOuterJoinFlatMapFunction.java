package com.splicemachine.derby.stream.function.merge;

import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.*;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iterator.merge.AbstractMergeJoinIterator;
import com.splicemachine.derby.stream.iterator.merge.MergeOuterJoinIterator;
import org.sparkproject.guava.common.collect.Iterators;
import org.sparkproject.guava.common.collect.PeekingIterator;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

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
    protected AbstractMergeJoinIterator createMergeJoinIterator(PeekingIterator<LocatedRow> leftPeekingIterator, PeekingIterator<LocatedRow> rightPeekingIterator, int[] leftHashKeys, int[] rightHashKeys, MergeJoinOperation mergeJoinOperation) {
        return new MergeOuterJoinIterator(leftPeekingIterator,
                rightPeekingIterator,
                mergeJoinOperation.leftHashKeys, mergeJoinOperation.rightHashKeys,
                mergeJoinOperation);
    }
}
