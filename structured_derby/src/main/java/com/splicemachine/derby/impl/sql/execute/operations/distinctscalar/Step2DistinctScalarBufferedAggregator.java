package com.splicemachine.derby.impl.sql.execute.operations.distinctscalar;

import com.splicemachine.derby.impl.sql.execute.SpliceExecutionFactory;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;
import com.splicemachine.derby.impl.sql.execute.operations.WarningCollector;
import com.splicemachine.derby.impl.sql.execute.operations.framework.AbstractBufferedAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.utils.StandardSupplier;
/**
 * 
 * Step 2 BufferedAggregator 
 * 
 *
 */
public class Step2DistinctScalarBufferedAggregator extends AbstractBufferedAggregator {
    private static final Logger LOG = Logger.getLogger(Step2DistinctScalarBufferedAggregator.class);
    private boolean isInitialized;

    protected Step2DistinctScalarBufferedAggregator(SpliceGenericAggregator[] aggregates,
                                 StandardSupplier<ExecRow> emptyRowSupplier,
                                 WarningCollector warningCollector) {
    	super(aggregates,emptyRowSupplier,warningCollector);
    }
    /**
     * Cloning Row to put into buffer.
     * 
     * 
     */
    public void initialize(ExecRow row) throws StandardException{
        this.currentRow = row.getClone();
        this.isInitialized = false;
				for(SpliceGenericAggregator aggregator:aggregates){
						if(aggregator.initialize(currentRow))
								aggregator.accumulate(currentRow,currentRow);
				}
    }
    /**
     * Merge Non Distinct and Distinct Aggregates
     * 
     */
    public void merge(ExecRow newRow) throws StandardException {
				for(SpliceGenericAggregator aggregator:aggregates){
//						aggregator.initializeAndAccumulateIfNeeded(newRow, newRow);
						aggregator.merge(newRow,currentRow);
				}
		}

    public boolean isInitialized() {
        return currentRow!=null;
    }

    public ExecRow finish() throws StandardException{
        if(currentRow==null)
            currentRow = emptyRowSupplier.get();
        ExecRow toReturn = currentRow;
        currentRow= null;
        return toReturn;
    }
}