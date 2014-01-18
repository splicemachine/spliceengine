package com.splicemachine.derby.impl.sql.execute.operations.distinctscalar;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;
import com.splicemachine.derby.impl.sql.execute.operations.WarningCollector;
import com.splicemachine.derby.impl.sql.execute.operations.framework.BufferedAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.utils.StandardSupplier;

public class DistinctScalarBufferedAggregator implements BufferedAggregator {
    private static final Logger LOG = Logger.getLogger(DistinctScalarBufferedAggregator.class);
    protected final SpliceGenericAggregator[] aggregates;
    private final StandardSupplier<ExecRow> emptyRowSupplier;
    private final WarningCollector warningCollector;
    private ExecRow currentRow;
    private boolean isSorted;
    private boolean alreadyInitialized;

    protected DistinctScalarBufferedAggregator(SpliceGenericAggregator[] aggregates,
                                 StandardSupplier<ExecRow> emptyRowSupplier,
                                 WarningCollector warningCollector, boolean isSorted, boolean alreadyInitialized) {
        this.aggregates= aggregates;
        this.emptyRowSupplier = emptyRowSupplier;
        this.warningCollector = warningCollector;
        this.isSorted = isSorted;
        this.alreadyInitialized = alreadyInitialized;
    }
    /**
     * Initialize Aggregator
     */
    public void initialize(ExecRow row) throws StandardException{
        this.currentRow = row.getClone();
        if (!alreadyInitialized) { // In the case we are reading from temp an already initialized aggregations.
	        for(SpliceGenericAggregator aggregator:aggregates){
	        	if (!aggregator.isDistinct() || isSorted) { // Only Initialize the distinct aggregates unless we are already sorted...
		            aggregator.initialize(row);
		            aggregator.accumulate(row,row);
	        	}
	        }
        }
    }
    /**
     * Merge Non Distinct Aggregates
     * 
     */
    public void merge(ExecRow newRow) throws StandardException{
    	for(SpliceGenericAggregator aggregator:aggregates) {
        	if (!aggregator.isDistinct() || isSorted) // Merge Non Distinct Aggregates and Distinct Aggregates if Source is Sorted
        		aggregator.merge(currentRow, newRow);
        }
    }

    public boolean isInitialized() {
        return currentRow!=null;
    }

    public ExecRow finish() throws StandardException{
        if(currentRow==null)
            currentRow = emptyRowSupplier.get();
        boolean eliminatedNulls = false;
        for(SpliceGenericAggregator aggregate:aggregates){
            if(aggregate.finish(currentRow))
                eliminatedNulls=true;
        }
        if(eliminatedNulls)
            warningCollector.addWarning(SQLState.LANG_NULL_ELIMINATED_IN_SET_FUNCTION);
        //once finished, return this to an unitialized state so it can be reused
        ExecRow toReturn = currentRow;
        currentRow= null;
        return toReturn;
    }
}