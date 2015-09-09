package com.splicemachine.derby.stream.function.merge;

import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.MergeJoinOperation;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iterator.merge.MergeAntiJoinIterator;
import org.sparkproject.guava.common.collect.Iterators;
import org.sparkproject.guava.common.collect.PeekingIterator;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by jleach on 6/9/15.
 */
public class MergeAntiJoinFlatMapFunction extends SpliceFlatMapFunction<MergeJoinOperation,Iterator<LocatedRow>,LocatedRow> {
    boolean initialized;
    protected MergeJoinOperation mergeJoinOperation;

    public MergeAntiJoinFlatMapFunction() {
        super();
    }

    public MergeAntiJoinFlatMapFunction(OperationContext<MergeJoinOperation> operationContext) {
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
    public Iterable<LocatedRow> call(Iterator<LocatedRow> locatedRows) throws Exception {
        PeekingIterator<LocatedRow> leftPeekingIterator = Iterators.peekingIterator(locatedRows);
        if (!initialized) {
            mergeJoinOperation = getOperation();
            initialized = true;
            if (!leftPeekingIterator.hasNext())
                return Collections.EMPTY_LIST;
            ((BaseActivation)mergeJoinOperation.getActivation()).setScanStartOverride(mergeJoinOperation.getKeyRow(leftPeekingIterator.peek().getRow()));
        }
        return new MergeAntiJoinIterator(leftPeekingIterator,
                Iterators.peekingIterator(mergeJoinOperation.getRightOperation().getDataSet().toLocalIterator()),
                mergeJoinOperation.leftHashKeys, mergeJoinOperation.rightHashKeys,
                mergeJoinOperation);
    }
}
