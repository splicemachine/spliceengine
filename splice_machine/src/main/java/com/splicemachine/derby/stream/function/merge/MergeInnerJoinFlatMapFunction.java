package com.splicemachine.derby.stream.function.merge;

import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.MergeJoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TableScanOperation;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iterator.TableScannerIterator;
import com.splicemachine.derby.stream.iterator.merge.MergeInnerJoinIterator;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.mrio.api.core.MultiRegionRemoteScanner;
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
public class MergeInnerJoinFlatMapFunction extends SpliceFlatMapFunction<MergeJoinOperation,Iterator<LocatedRow>,LocatedRow> {
    boolean initialized;
    protected MergeJoinOperation mergeJoinOperation;

    public MergeInnerJoinFlatMapFunction() {
        super();
    }

    public MergeInnerJoinFlatMapFunction(OperationContext<MergeJoinOperation> operationContext) {
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
        TableScanOperation rightSide = (TableScanOperation)mergeJoinOperation.getRightOperation();
        DataSetProcessor dsp = StreamUtils.getDataSetProcessorFromActivation(getOperation().getActivation(), rightSide);
        TableScannerIterator rightIterator = dsp.getTableScannerIterator((TableScanOperation) mergeJoinOperation.getRightOperation());

        MergeInnerJoinIterator iterator = new MergeInnerJoinIterator(leftPeekingIterator,
                Iterators.peekingIterator(rightIterator),
                mergeJoinOperation.leftHashKeys, mergeJoinOperation.rightHashKeys,
                mergeJoinOperation);
        iterator.registerCloseable(rightIterator);
        return iterator;
    }
}
