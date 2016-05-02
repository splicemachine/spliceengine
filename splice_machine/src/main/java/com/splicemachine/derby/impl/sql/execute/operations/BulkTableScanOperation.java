package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

/**
 * Wraps TableScanOperation.  We do not really have a different between bulk and a table scan.  We attempt to 
 * parallalize (bulk) anything we can already.  
 * 
 * @author johnleach
 *
 */

public class BulkTableScanOperation extends TableScanOperation {
	private static Logger LOG = Logger.getLogger(BulkTableScanOperation.class);
	public BulkTableScanOperation() {
		super();
	}
	
    protected static final String NAME = BulkTableScanOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}


	public BulkTableScanOperation(long conglomId,
			StaticCompiledOpenConglomInfo scoci, Activation activation, 
			GeneratedMethod resultRowAllocator, 
			int resultSetNumber,
			GeneratedMethod startKeyGetter, int startSearchOperator,
			GeneratedMethod stopKeyGetter, int stopSearchOperator,
			boolean sameStartStopPosition,
            boolean rowIdKey,
			String qualifiersField,
			String tableName,
			String userSuppliedOptimizerOverrides,
			String indexName,
			boolean isConstraint,
			boolean forUpdate,
			int colRefItem,
			int indexColItem,
			int lockMode,
			boolean tableLocked,
			int isolationLevel,
			int rowsPerRead,
	        boolean disableForHoldable,
			boolean oneRowScan,
			double optimizerEstimatedRowCount,
			double optimizerEstimatedCost)
				throws StandardException
	    {
			super(conglomId,
				scoci,
				activation,
				resultRowAllocator,
				resultSetNumber,
				startKeyGetter,
				startSearchOperator,
				stopKeyGetter,
				stopSearchOperator,
				sameStartStopPosition,
                rowIdKey,
				qualifiersField,
				tableName,
				userSuppliedOptimizerOverrides,
				indexName,
				isConstraint,
				forUpdate,
				colRefItem,
				indexColItem,
				lockMode,
				tableLocked,
				isolationLevel,
	            adjustBulkFetchSize(activation, rowsPerRead, disableForHoldable),
				oneRowScan,
				optimizerEstimatedRowCount,
				optimizerEstimatedCost);
			SpliceLogUtils.trace(LOG,"Instantiating");
			recordConstructorTime(); 
	}
    private static int adjustBulkFetchSize(Activation activation, int rowsPerRead, boolean disableForHoldable){
        if (disableForHoldable && activation.getResultSetHoldability()) {
            // We have a holdable cursor, and we've been requested to disable
            // bulk fetch if the cursor is holdable, so change bulk size to 1.
            return 1;
        } else {
            return rowsPerRead;
        }
    }
}
