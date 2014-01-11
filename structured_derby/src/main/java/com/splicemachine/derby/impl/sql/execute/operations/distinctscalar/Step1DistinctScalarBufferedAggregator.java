package com.splicemachine.derby.impl.sql.execute.operations.distinctscalar;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;
import com.splicemachine.derby.impl.sql.execute.operations.WarningCollector;
import com.splicemachine.derby.impl.sql.execute.operations.framework.AbstractBufferedAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.utils.StandardSupplier;
/*
 * This Aggregator handles the merging of non-sorted distinct and non-distinct aggregates
 * 
 * 
 */
public class Step1DistinctScalarBufferedAggregator extends AbstractBufferedAggregator {
    private static final Logger LOG = Logger.getLogger(Step1DistinctScalarBufferedAggregator.class);
    private boolean isInitialized;

    protected Step1DistinctScalarBufferedAggregator(SpliceGenericAggregator[] aggregates,
                                 StandardSupplier<ExecRow> emptyRowSupplier,
                                 WarningCollector warningCollector) {
    	super(aggregates,emptyRowSupplier,warningCollector);
    }
    /**
     * Clones the row when going into the buffer.
     * 
     * 
     */
    public void initialize(ExecRow row) throws StandardException{
        this.currentRow = row.getClone();
        this.isInitialized = false;
    }
    /**
     * Merge Non Distinct Aggregates
     * 
     */
    public void merge(ExecRow newRow) throws StandardException{
    	for(SpliceGenericAggregator aggregator:aggregates){
    	    if (!aggregator.isDistinct()) { // Some aggregates that are distinct come accross as non-distinct. XXX TODO JLEACH
    	       	if (!isInitialized) {
    	       		aggregator.initialize(currentRow);
    	       		aggregator.accumulate(currentRow,currentRow);
    	       	}
    	       	aggregator.initializeAndAccumulateIfNeeded(newRow, newRow);
    	       	aggregator.merge(newRow,currentRow);
    	    }
        }
   		isInitialized = true;
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