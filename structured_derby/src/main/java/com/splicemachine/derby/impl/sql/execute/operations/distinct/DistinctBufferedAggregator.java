package com.splicemachine.derby.impl.sql.execute.operations.distinct;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.framework.AbstractBufferedAggregator;
import com.splicemachine.derby.utils.StandardSupplier;

public class DistinctBufferedAggregator extends AbstractBufferedAggregator {
    protected DistinctBufferedAggregator(StandardSupplier<ExecRow> emptyRowSupplier) {
    	super(null,emptyRowSupplier,null);
    }

    public void initialize(ExecRow row) throws StandardException{
        this.currentRow = row.getClone();
    }

    public void merge(ExecRow newRow) throws StandardException{
    	// No Op
    }

    public boolean isInitialized() {
        return currentRow!=null;
    }

    public ExecRow finish() throws StandardException{
        if(currentRow==null)
            currentRow = emptyRowSupplier.get();
        //once finished, return this to an unitialized state so it can be reused
        ExecRow toReturn = currentRow;
        currentRow= null;
        return toReturn;
    }
}