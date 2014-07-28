package com.splicemachine.derby.impl.sql.execute.operations.window;

import com.splicemachine.derby.impl.sql.execute.operations.AggregateContext;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.utils.PartitionAwarePushBackIterator;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

/**
 * Created by jyuan on 7/27/14.
 */
public class WindowFunctionWorker {
    PartitionAwarePushBackIterator<ExecRow> source;
    private byte[] partitionKey;
    AggregateContext aggregateContext;
    WindowFrameDefinition frame;
    SpliceGenericAggregator[] aggregators;

    public WindowFunctionWorker() {

    }

    public WindowFunctionWorker(PartitionAwarePushBackIterator<ExecRow> source,
                                WindowFrameDefinition frame,
                                AggregateContext aggregateContext) throws StandardException{
        this.source = source;
        this.partitionKey = source.getPartition();
        this.frame = frame;
        this.aggregateContext = aggregateContext;
        initAggregators();
    }

    private void initAggregators() throws StandardException{
        aggregators = aggregateContext.getAggregators();
        for (SpliceGenericAggregator aggregator:aggregators) {
            aggregator.getAggregatorInstance();
        }
    }

    
}
