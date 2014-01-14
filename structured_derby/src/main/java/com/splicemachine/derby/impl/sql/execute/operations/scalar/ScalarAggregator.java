package com.splicemachine.derby.impl.sql.execute.operations.scalar;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 10/8/13
 */
public class ScalarAggregator {

    private final ScalarAggregateSource source;
    private final boolean shouldMerge;
    private final boolean initialize;
    private final SpliceGenericAggregator[] aggregates;
    private long rowsRead;

    public ScalarAggregator(ScalarAggregateSource source,SpliceGenericAggregator[] aggregates,
                            boolean shouldMerge, boolean initialize
                            ) {
        this.source = source;
        this.shouldMerge = shouldMerge;
        this.initialize = initialize;
        this.aggregates = aggregates;
    }

    public ExecRow aggregate(SpliceRuntimeContext spliceRuntimeContext) throws StandardException,IOException{
        ExecIndexRow nextRow;
        ExecRow aggResult = null;
        do {
			SpliceBaseOperation.checkInterrupt(rowsRead,SpliceConstants.interruptLoopCheck);
			nextRow = source.nextRow(spliceRuntimeContext);
            if(nextRow==null)continue;
            aggResult = aggregate(nextRow,aggResult);
            rowsRead++;
        }while(nextRow!=null);
        return aggResult;
		}

		public boolean finish(ExecRow input) throws StandardException{
        boolean eliminatedNulls = false;
        for (SpliceGenericAggregator currAggregate : aggregates) {
            if (currAggregate.finish(input))
                eliminatedNulls = true;
        }
        return eliminatedNulls;
    }

    private ExecRow aggregate(ExecIndexRow indexRow,ExecRow aggResult) throws StandardException,IOException{
        if(aggResult==null){
            aggResult = indexRow.getClone();
            if(initialize){
                initialize(aggResult);
            }
            return aggResult;
        }

        for(SpliceGenericAggregator aggregate:aggregates){
            if(shouldMerge){
                aggregate.merge(indexRow, aggResult);
            }else{
                aggregate.accumulate(indexRow,aggResult);
            }
        }
        return aggResult;
    }

    private void initialize(ExecRow aggResult) throws StandardException{
        for(SpliceGenericAggregator aggregator:aggregates){
            aggregator.initialize(aggResult);
            aggregator.accumulate(aggResult,aggResult);
        }
    }

}
