package com.splicemachine.derby.impl.sql.execute.operations.sort;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;

import com.splicemachine.derby.impl.sql.execute.operations.SortOperation;
import com.splicemachine.derby.impl.sql.execute.operations.framework.BufferedAggregator;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.utils.SpliceLogUtils;
/*
 * This handles distinct merging of data.
 * 
 * 
 */
public class DistinctSortBufferedAggregator implements BufferedAggregator {
    private static Logger LOG = Logger.getLogger(DistinctSortBufferedAggregator.class);
    protected ExecRow currentRow;
    protected final StandardSupplier<ExecRow> emptyRowSupplier;
    
    public DistinctSortBufferedAggregator(StandardSupplier<ExecRow> emptyRowSupplier) {
    	this.emptyRowSupplier = emptyRowSupplier;
    }
    /**
     * Clones the row when going into the buffer.
     * 
     * 
     */
    public void initialize(ExecRow row) throws StandardException {
        this.currentRow = row.getClone();
    }
    /**
     * Merge Non Distinct Aggregates
     * 
     */
    public void merge(ExecRow newRow) throws StandardException {
    	if (LOG.isTraceEnabled()) {
    		SpliceLogUtils.trace(LOG, "discarding row ",newRow);
    	}
    	// throw away row
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