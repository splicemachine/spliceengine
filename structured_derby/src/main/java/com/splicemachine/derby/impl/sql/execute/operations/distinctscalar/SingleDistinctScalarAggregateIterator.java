package com.splicemachine.derby.impl.sql.execute.operations.distinctscalar;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.WarningCollector;
import com.splicemachine.derby.impl.sql.execute.operations.framework.AbstractStandardIterator;
import com.splicemachine.derby.impl.sql.execute.operations.framework.EmptyRowSupplier;
import com.splicemachine.derby.impl.sql.execute.operations.framework.GroupedRow;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.utils.StandardIterator;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 11/1/13
 */
public class SingleDistinctScalarAggregateIterator extends AbstractStandardIterator {
    private ExecRow currentRow;
    private EmptyRowSupplier emptyRowSupplier;
    private SpliceGenericAggregator[] aggregates;
    private WarningCollector warningCollector;

    
    public SingleDistinctScalarAggregateIterator(StandardIterator<ExecRow> source, EmptyRowSupplier emptyRowSupplier, WarningCollector warningCollector, SpliceGenericAggregator[] aggregates) {
    	super(source);
        this.emptyRowSupplier = emptyRowSupplier;
        this.aggregates = aggregates;
        this.warningCollector = warningCollector;
    }
    
    public void merge(ExecRow newRow) throws StandardException{
    	for(SpliceGenericAggregator aggregator:aggregates) {
    		initialize(newRow);
    		aggregator.merge(newRow, currentRow);
        }
    }

    public void initialize(ExecRow newRow) throws StandardException{
    	for(SpliceGenericAggregator aggregator:aggregates) {
    		aggregator.initializeAndAccumulateIfNeeded(newRow,newRow);
        }
    }

    @Override
    public GroupedRow next(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
    	ExecRow nextRow = source.next(spliceRuntimeContext);
    	if (nextRow == null) {// Return Default Values...
        	currentRow = emptyRowSupplier.get();
        	finish();
        	return new GroupedRow(currentRow,new byte[0]);        			
        }
        currentRow = nextRow.getClone();
        initialize(currentRow);
        boolean shouldContinue = true;
    	do{
	            nextRow = source.next(spliceRuntimeContext);
	            shouldContinue = nextRow!=null;
	            if(!shouldContinue)
	                continue; //iterator exhausted, break from the loop
	            merge(nextRow);
    	}while(shouldContinue);	
    	finish();
    	
    	return new GroupedRow(currentRow,new byte[0]);
    }
    
    public void finish() throws StandardException{
        if(currentRow==null)
            currentRow = emptyRowSupplier.get();
        boolean eliminatedNulls = false;
        for(SpliceGenericAggregator aggregate:aggregates){
            if(aggregate.finish(currentRow))
                eliminatedNulls=true;
        }
        if(eliminatedNulls)
            warningCollector.addWarning(SQLState.LANG_NULL_ELIMINATED_IN_SET_FUNCTION);
    }
}
